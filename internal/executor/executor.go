// Package executor runs validated control-plane commands against the
// cluster: apply/patch/delete with drift gates and dry-run-first, node
// drains, Helm operations and cert-manager certificate rotation.
package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/yaml"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/drift"
	"AuditReady-k3s/internal/metrics"
	"AuditReady-k3s/internal/protocol"
)

// Terminal results for metrics.IncCommand.
const (
	resultSucceeded = "succeeded"
	resultFailed    = "failed"
	resultRefused   = "refused"
)

// Executor applies commands to the cluster via dynamic and typed clients.
type Executor struct {
	cfg     *config.Config
	dyn     dynamic.Interface
	kube    kubernetes.Interface
	restCfg *rest.Config
	mapper  meta.RESTMapper
	log     *slog.Logger
}

// New returns an Executor. restCfg is used for Helm operations and to build
// the discovery RESTMapper that apply uses to resolve payload kinds to
// resources. Tests substitute a static mapper via the mapper field.
func New(cfg *config.Config, dyn dynamic.Interface, kube kubernetes.Interface, restCfg *rest.Config, log *slog.Logger) *Executor {
	if log == nil {
		log = slog.Default()
	}
	e := &Executor{cfg: cfg, dyn: dyn, kube: kube, restCfg: restCfg, log: log}
	if dc, err := discovery.NewDiscoveryClientForConfig(restCfg); err != nil {
		// apply falls back to the command target when no mapper is available.
		log.Warn("discovery client unavailable, apply cannot resolve payload kinds", "error", err)
	} else {
		e.mapper = restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))
	}
	return e
}

// HandleCommand executes cmd and reports every stage through report. The
// callback stamps ClusterID/CommandID/Timestamp; the executor fills
// Status/Message/Diff/Progress.
func (e *Executor) HandleCommand(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report)) {
	log := e.log.With("command", cmd.ID, "verb", cmd.Verb)
	log.Info("handling command", "target", fmt.Sprintf("%s/%s", cmd.Target.Namespace, cmd.Target.Name))
	report(&protocol.Report{Status: protocol.StatusValidating})

	switch cmd.Verb {
	case protocol.VerbApply:
		e.apply(ctx, cmd, report, log)
	case protocol.VerbPatch:
		e.patch(ctx, cmd, report, log)
	case protocol.VerbDelete:
		e.delete(ctx, cmd, report, log)
	case protocol.VerbDrainNode:
		e.drainNode(ctx, cmd, report, log)
	case protocol.VerbUncordonNode:
		e.uncordonNode(ctx, cmd, report, log)
	case protocol.VerbHelmInstall:
		e.helmInstall(ctx, cmd, report, log)
	case protocol.VerbHelmUpgrade:
		e.helmUpgrade(ctx, cmd, report, log)
	case protocol.VerbHelmUninstall:
		e.helmUninstall(ctx, cmd, report, log)
	case protocol.VerbCertRotate:
		e.certRotate(ctx, cmd, report, log)
	case protocol.VerbClusterUpgrade:
		// Defensive guard: the validator refuses this already, but never
		// let it through even if the gate order changes.
		e.refuse(cmd, report, "cluster-upgrade is not supported by this agent version")
	default:
		e.refuse(cmd, report, fmt.Sprintf("unknown verb %q", cmd.Verb))
	}
}

func (e *Executor) refuse(cmd *protocol.Command, report func(*protocol.Report), reason string) {
	metrics.IncCommand(cmd.Verb, resultRefused)
	report(&protocol.Report{Status: protocol.StatusRefused, Message: reason})
}

func (e *Executor) fail(cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger, msg string, err error) {
	log.Warn("command failed", "error", err)
	metrics.IncCommand(cmd.Verb, resultFailed)
	report(&protocol.Report{Status: protocol.StatusFailed, Message: fmt.Sprintf("%s: %v", msg, err)})
}

func (e *Executor) succeed(cmd *protocol.Command, report func(*protocol.Report), msg string) {
	metrics.IncCommand(cmd.Verb, resultSucceeded)
	report(&protocol.Report{Status: protocol.StatusSucceeded, Message: msg})
}

func gvrOf(ref protocol.ResourceRef) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: ref.Group, Version: ref.Version, Resource: ref.Resource}
}

// checkDrift applies the configured drift policy when cmd.ExpectedHash is
// set. It returns proceed=false after emitting a refusal report.
func (e *Executor) checkDrift(cmd *protocol.Command, live *unstructured.Unstructured, report func(*protocol.Report), log *slog.Logger) (proceed bool) {
	if cmd.ExpectedHash == "" {
		return true
	}
	match, diff := drift.Check(live, cmd.ExpectedHash)
	if match {
		return true
	}
	switch e.cfg.DriftPolicy {
	case config.DriftOverwrite:
		log.Warn("drift detected, overwriting per policy", "diff", diff)
		return true
	case config.DriftOverwriteWithApproval:
		if cmd.Override {
			log.Warn("drift detected, overwriting with approval", "diff", diff)
			return true
		}
		metrics.IncCommand(cmd.Verb, resultRefused)
		report(&protocol.Report{Status: protocol.StatusRefused, Message: "drift detected, approval required (override not set)", Diff: diff})
		return false
	default: // config.DriftRefuse
		metrics.IncCommand(cmd.Verb, resultRefused)
		report(&protocol.Report{Status: protocol.StatusRefused, Message: "drift detected, refusing to overwrite", Diff: diff})
		return false
	}
}

// liveGet fetches the current object; nil, nil when it does not exist.
func liveGet(ctx context.Context, ri dynamic.ResourceInterface, name string) (*unstructured.Unstructured, error) {
	live, err := ri.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return live, err
}

// liveObject fetches the current object; nil, nil when it does not exist.
func (e *Executor) liveObject(ctx context.Context, gvr schema.GroupVersionResource, ns, name string) (*unstructured.Unstructured, error) {
	return liveGet(ctx, e.dyn.Resource(gvr).Namespace(ns), name)
}

// decodeManifest splits a possibly multi-document YAML payload into objects,
// skipping empty documents and expanding kind: List wrappers.
func decodeManifest(payload []byte) ([]*unstructured.Unstructured, error) {
	var objs []*unstructured.Unstructured
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(payload)))
	for {
		doc, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		doc = bytes.TrimSpace(doc)
		if len(doc) == 0 {
			continue
		}
		var m map[string]interface{}
		if err := yaml.Unmarshal(doc, &m); err != nil {
			return nil, err
		}
		if len(m) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{Object: m}
		if obj.GetKind() == "List" {
			items, _, err := unstructured.NestedSlice(m, "items")
			if err != nil {
				return nil, fmt.Errorf("cannot read List items: %w", err)
			}
			for _, item := range items {
				im, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("List item is not an object")
				}
				objs = append(objs, &unstructured.Unstructured{Object: im})
			}
			continue
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

// gvrFor resolves the resource for one payload object from its own
// apiVersion/kind via discovery, so apply works with the empty command
// target the control plane sends for manifest deployments. Falls back to the
// command target when the object carries no kind or discovery has no match.
func (e *Executor) gvrFor(obj *unstructured.Unstructured, fallback protocol.ResourceRef) (gvr schema.GroupVersionResource, namespaced bool, err error) {
	gvk := obj.GroupVersionKind()
	if e.mapper != nil && gvk.Kind != "" {
		mapping, merr := e.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if merr == nil {
			return mapping.Resource, mapping.Scope.Name() != meta.RESTScopeNameRoot, nil
		}
		if !meta.IsNoMatchError(merr) {
			return schema.GroupVersionResource{}, false, merr
		}
	}
	if fallback.Resource != "" && fallback.Version != "" {
		// Target-specified commands predate kind resolution and address
		// namespaced resources only.
		return gvrOf(fallback), true, nil
	}
	return schema.GroupVersionResource{}, false, fmt.Errorf(
		"cannot resolve resource type for %s %q (apiVersion %q): not in discovery and no command target",
		gvk.Kind, obj.GetName(), gvk.GroupVersion().String())
}

// apply decodes the payload (one or more YAML documents), drift-checks each
// live object and creates-or-updates it, dry-run first when configured. The
// GVR comes from each object's own apiVersion/kind via discovery — apply
// commands carry an empty target.
func (e *Executor) apply(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	objs, err := decodeManifest(cmd.Payload)
	if err != nil {
		e.fail(cmd, report, log, "cannot decode payload", err)
		return
	}
	if len(objs) == 0 {
		e.fail(cmd, report, log, "cannot decode payload", fmt.Errorf("payload contains no objects"))
		return
	}

	created, updated := 0, 0
	for _, obj := range objs {
		if obj.GetName() == "" {
			obj.SetName(cmd.Target.Name)
		}
		if obj.GetNamespace() == "" {
			obj.SetNamespace(cmd.Target.Namespace)
		}
		gvr, namespaced, err := e.gvrFor(obj, cmd.Target)
		if err != nil {
			e.fail(cmd, report, log, "cannot resolve resource type", err)
			return
		}
		ns, name := obj.GetNamespace(), obj.GetName()
		id := fmt.Sprintf("%s %s/%s", obj.GetKind(), ns, name)

		var ri dynamic.ResourceInterface = e.dyn.Resource(gvr)
		if namespaced {
			ri = e.dyn.Resource(gvr).Namespace(ns)
		}

		live, err := liveGet(ctx, ri, name)
		if err != nil {
			e.fail(cmd, report, log, "cannot fetch live "+id, err)
			return
		}
		if live != nil && !e.checkDrift(cmd, live, report, log) {
			return
		}

		drift.Stamp(obj)

		op := "create"
		if live != nil {
			op = "update"
			obj.SetResourceVersion(live.GetResourceVersion())
		}

		if e.cfg.DryRunFirst {
			if err := e.createOrUpdate(ctx, ri, obj, op, true); err != nil {
				e.fail(cmd, report, log, "dry-run "+op+" of "+id+" failed", err)
				return
			}
			report(&protocol.Report{Status: protocol.StatusDryRun, Message: fmt.Sprintf("dry-run %s of %s validated", op, id)})
		}

		report(&protocol.Report{Status: protocol.StatusApplying, Message: op + " " + id})
		if err := e.createOrUpdate(ctx, ri, obj, op, false); err != nil {
			e.fail(cmd, report, log, op+" of "+id+" failed", err)
			return
		}
		if live != nil {
			updated++
		} else {
			created++
		}
	}
	e.succeed(cmd, report, fmt.Sprintf("applied %d object(s): %d created, %d updated", created+updated, created, updated))
}

func (e *Executor) createOrUpdate(ctx context.Context, ri dynamic.ResourceInterface, obj *unstructured.Unstructured, op string, dryRun bool) error {
	dry := []string(nil)
	if dryRun {
		dry = []string{metav1.DryRunAll}
	}
	if op == "create" {
		_, err := ri.Create(ctx, obj, metav1.CreateOptions{DryRun: dry})
		return err
	}
	_, err := ri.Update(ctx, obj, metav1.UpdateOptions{DryRun: dry})
	return err
}

// patch applies a strategic/merge/json patch to the target, dry-run first
// when configured, then re-stamps the drift hash annotation.
func (e *Executor) patch(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	var pt types.PatchType
	switch cmd.PatchType {
	case protocol.PatchStrategic:
		pt = types.StrategicMergePatchType
	case protocol.PatchMerge:
		pt = types.MergePatchType
	case protocol.PatchJSON:
		pt = types.JSONPatchType
	default:
		e.refuse(cmd, report, fmt.Sprintf("unsupported patch type %q", cmd.PatchType))
		return
	}

	gvr := gvrOf(cmd.Target)
	ns, name := cmd.Target.Namespace, cmd.Target.Name

	live, err := e.liveObject(ctx, gvr, ns, name)
	if err != nil {
		e.fail(cmd, report, log, "cannot fetch live object", err)
		return
	}
	if live == nil {
		e.fail(cmd, report, log, "target not found", fmt.Errorf("%s %s/%s does not exist", cmd.Target.Resource, ns, name))
		return
	}
	if !e.checkDrift(cmd, live, report, log) {
		return
	}

	ri := e.dyn.Resource(gvr).Namespace(ns)
	if e.cfg.DryRunFirst {
		if _, err := ri.Patch(ctx, name, pt, cmd.Payload, metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
			e.fail(cmd, report, log, "dry-run patch failed", err)
			return
		}
		report(&protocol.Report{Status: protocol.StatusDryRun, Message: fmt.Sprintf("dry-run patch of %s %s/%s validated", cmd.Target.Resource, ns, name)})
	}

	report(&protocol.Report{Status: protocol.StatusApplying, Message: "patch " + name})
	if _, err := ri.Patch(ctx, name, pt, cmd.Payload, metav1.PatchOptions{}); err != nil {
		e.fail(cmd, report, log, "patch failed", err)
		return
	}

	// Re-stamp the hash annotation so the next command's ExpectedHash
	// matches the new state.
	if live, err := e.liveObject(ctx, gvr, ns, name); err == nil && live != nil {
		drift.Stamp(live)
		stamp, merr := json.Marshal(map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": map[string]string{drift.HashAnnotation: live.GetAnnotations()[drift.HashAnnotation]},
			},
		})
		if merr == nil {
			if _, perr := ri.Patch(ctx, name, types.MergePatchType, stamp, metav1.PatchOptions{}); perr != nil {
				log.Warn("could not re-stamp drift hash", "error", perr)
			}
		}
	}
	e.succeed(cmd, report, fmt.Sprintf("patched %s %s/%s", cmd.Target.Resource, ns, name))
}

// delete removes the target, dry-run first when configured. Deleting an
// already-absent object counts as success.
func (e *Executor) delete(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	gvr := gvrOf(cmd.Target)
	ns, name := cmd.Target.Namespace, cmd.Target.Name

	live, err := e.liveObject(ctx, gvr, ns, name)
	if err != nil {
		e.fail(cmd, report, log, "cannot fetch live object", err)
		return
	}
	if live == nil {
		e.succeed(cmd, report, fmt.Sprintf("%s %s/%s already absent", cmd.Target.Resource, ns, name))
		return
	}
	if !e.checkDrift(cmd, live, report, log) {
		return
	}

	ri := e.dyn.Resource(gvr).Namespace(ns)
	if e.cfg.DryRunFirst {
		if err := ri.Delete(ctx, name, metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}); err != nil && !apierrors.IsNotFound(err) {
			e.fail(cmd, report, log, "dry-run delete failed", err)
			return
		}
		report(&protocol.Report{Status: protocol.StatusDryRun, Message: fmt.Sprintf("dry-run delete of %s %s/%s validated", cmd.Target.Resource, ns, name)})
	}

	report(&protocol.Report{Status: protocol.StatusApplying, Message: "delete " + name})
	if err := ri.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		e.fail(cmd, report, log, "delete failed", err)
		return
	}
	e.succeed(cmd, report, fmt.Sprintf("deleted %s %s/%s", cmd.Target.Resource, ns, name))
}

// drainNode cordons the node and evicts every pod except mirror, DaemonSet
// and protected-namespace pods (the agent's own namespace is protected, so a
// drain can never evict the agent itself out from under its own command). A
// stalled drain (PDB) pauses for human intervention; the node is never
// uncordoned automatically.
func (e *Executor) drainNode(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	node := cmd.Target.Name
	if node == "" {
		e.fail(cmd, report, log, "drain-node requires a node name", fmt.Errorf("empty target name"))
		return
	}

	if _, err := e.kube.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{}); err != nil {
		e.fail(cmd, report, log, "cannot get node", err)
		return
	}
	// Cordon.
	if _, err := e.kube.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType,
		[]byte(`{"spec":{"unschedulable":true}}`), metav1.PatchOptions{}); err != nil {
		e.fail(cmd, report, log, "cannot cordon node", err)
		return
	}
	log.Info("node cordoned", "node", node)

	pods, err := e.kube.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		e.fail(cmd, report, log, "cannot list pods", err)
		return
	}

	// The fake clientset ignores field selectors, so filter explicitly by
	// node name, and skip mirror, DaemonSet-managed and protected-namespace
	// pods.
	total := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName != node || isMirrorPod(p) || isDaemonSetPod(p) || e.cfg.ProtectedNamespace(p.Namespace) {
			continue
		}
		total++
	}
	if total == 0 {
		metrics.ClearDrainProgress(node)
		e.succeed(cmd, report, fmt.Sprintf("node %s drained (no evictable pods)", node))
		return
	}

	lastSuccess := time.Now()
	evicted := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName != node || isMirrorPod(p) || isDaemonSetPod(p) || e.cfg.ProtectedNamespace(p.Namespace) {
			continue
		}
		for {
			err := e.kube.PolicyV1().Evictions(p.Namespace).Evict(ctx, &policyv1.Eviction{
				ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace},
			})
			if err == nil || apierrors.IsNotFound(err) {
				break
			}
			if time.Since(lastSuccess) >= e.cfg.DrainStallTimeout {
				// Paused, not failed: leave the node cordoned and wait for
				// a human. No terminal report, no metric.
				log.Warn("drain stalled, waiting for human", "node", node, "pod", p.Name, "error", err)
				report(&protocol.Report{
					Status:   protocol.StatusProgress,
					Message:  fmt.Sprintf("drain stalled, waiting for human: cannot evict %s/%s: %v", p.Namespace, p.Name, err),
					Progress: int32(evicted * 100 / total),
				})
				return
			}
			select {
			case <-ctx.Done():
				e.fail(cmd, report, log, "drain cancelled", ctx.Err())
				return
			case <-time.After(2 * time.Second):
			}
		}
		lastSuccess = time.Now()
		evicted++
		pct := int32(evicted * 100 / total)
		metrics.SetDrainProgress(node, float64(pct))
		report(&protocol.Report{
			Status:   protocol.StatusProgress,
			Message:  fmt.Sprintf("evicted %s/%s (%d/%d)", p.Namespace, p.Name, evicted, total),
			Progress: pct,
		})
	}

	metrics.ClearDrainProgress(node)
	e.succeed(cmd, report, fmt.Sprintf("node %s drained, %d pods evicted", node, evicted))
}

// uncordonNode marks the node schedulable again. Uncordoning an
// already-schedulable node counts as success.
func (e *Executor) uncordonNode(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	node := cmd.Target.Name
	if node == "" {
		e.fail(cmd, report, log, "uncordon-node requires a node name", fmt.Errorf("empty target name"))
		return
	}

	if _, err := e.kube.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{}); err != nil {
		e.fail(cmd, report, log, "cannot get node", err)
		return
	}
	if _, err := e.kube.CoreV1().Nodes().Patch(ctx, node, types.MergePatchType,
		[]byte(`{"spec":{"unschedulable":false}}`), metav1.PatchOptions{}); err != nil {
		e.fail(cmd, report, log, "cannot uncordon node", err)
		return
	}
	log.Info("node uncordoned", "node", node)
	e.succeed(cmd, report, fmt.Sprintf("node %s uncordoned", node))
}

func isMirrorPod(p *corev1.Pod) bool {
	_, ok := p.Annotations["kubernetes.io/config.mirror"]
	return ok
}

func isDaemonSetPod(p *corev1.Pod) bool {
	for _, ref := range p.OwnerReferences {
		if ref.Kind == "DaemonSet" && ref.APIVersion == "apps/v1" {
			return true
		}
	}
	return false
}

// certRotate forces cert-manager to re-issue a Certificate by creating a
// fresh CertificateRequest that references it.
func (e *Executor) certRotate(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	certs := schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
	ns, name := cmd.Target.Namespace, cmd.Target.Name

	cert, err := e.dyn.Resource(certs).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		e.fail(cmd, report, log, "cannot fetch Certificate", err)
		return
	}

	issuerRef, _, _ := unstructured.NestedMap(cert.Object, "spec", "issuerRef")
	if issuerRef == nil {
		e.fail(cmd, report, log, "Certificate has no spec.issuerRef", fmt.Errorf("%s/%s", ns, name))
		return
	}
	usages, _, _ := unstructured.NestedStringSlice(cert.Object, "spec", "usages")
	if len(usages) == 0 {
		usages = []string{"digital signature", "key encipherment"}
	}
	usageVals := make([]interface{}, len(usages))
	for i, u := range usages {
		usageVals[i] = u
	}

	crName := fmt.Sprintf("%s-renew-%d", name, time.Now().Unix())
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "CertificateRequest",
		"metadata": map[string]interface{}{
			"name":      crName,
			"namespace": ns,
			"annotations": map[string]interface{}{
				"cert-manager.io/certificate-name": name,
			},
		},
		"spec": map[string]interface{}{
			"issuerRef": issuerRef,
			"usages":    usageVals,
		},
	}}

	crs := schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificaterequests"}
	ri := e.dyn.Resource(crs).Namespace(ns)

	if e.cfg.DryRunFirst {
		if _, err := ri.Create(ctx, cr, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
			e.fail(cmd, report, log, "dry-run create CertificateRequest failed", err)
			return
		}
		report(&protocol.Report{Status: protocol.StatusDryRun, Message: fmt.Sprintf("dry-run create of CertificateRequest %s/%s validated", ns, crName)})
	}

	report(&protocol.Report{Status: protocol.StatusApplying, Message: "create CertificateRequest " + crName})
	if _, err := ri.Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		e.fail(cmd, report, log, "create CertificateRequest failed", err)
		return
	}
	e.succeed(cmd, report, fmt.Sprintf("created CertificateRequest %s/%s for Certificate %s", ns, crName, name))
}
