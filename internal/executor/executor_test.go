package executor

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	kubefake "k8s.io/client-go/kubernetes/fake"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/drift"
	"AuditReady-k3s/internal/protocol"
)

var cmGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
var pvcGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}
var svcGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}

func testConfig() *config.Config {
	return &config.Config{
		ClusterID:           "test",
		WriterEnabled:       true,
		DryRunFirst:         true,
		DriftPolicy:         config.DriftRefuse,
		AllowDelete:         true,
		ProtectedNamespaces: []string{"kube-system"},
		DrainStallTimeout:   time.Minute,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// recorder captures reports in order.
type recorder struct {
	mu      sync.Mutex
	reports []*protocol.Report
}

func (r *recorder) report(rep *protocol.Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cpy := *rep
	r.reports = append(r.reports, &cpy)
}

func (r *recorder) statuses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.reports))
	for i, rep := range r.reports {
		out[i] = rep.Status
	}
	return out
}

func (r *recorder) last() *protocol.Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reports[len(r.reports)-1]
}

func applyCommand() *protocol.Command {
	return &protocol.Command{
		ID:   "cmd-1",
		Verb: protocol.VerbApply,
		Target: protocol.ResourceRef{
			Version: "v1", Resource: "configmaps", Namespace: "default", Name: "cm",
		},
		Payload: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: default\ndata:\n  key: desired\n"),
	}
}

func newExecutor(cfg *config.Config, dyn *dynfake.FakeDynamicClient, kube *kubefake.Clientset) *Executor {
	e := New(cfg, dyn, kube, &rest.Config{Host: "https://127.0.0.1:6443"}, testLogger())
	// The deferred discovery mapper built by New would dial the fake host;
	// substitute a static mapper for the kinds the tests use.
	e.mapper = testRESTMapper()
	return e
}

// testRESTMapper is a static mapper for the kinds used in tests, pluralized
// with the standard unsafe guess (ConfigMap→configmaps, …).
func testRESTMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	namespaced := []schema.GroupVersionKind{
		{Group: "", Version: "v1", Kind: "ConfigMap"},
		{Group: "", Version: "v1", Kind: "Secret"},
		{Group: "", Version: "v1", Kind: "Service"},
		{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
	}
	for _, gvk := range namespaced {
		m.Add(gvk, meta.RESTScopeNamespace)
	}
	m.Add(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}, meta.RESTScopeRoot)
	return m
}

// honorCreateDryRun makes the fake dynamic client skip persisting dry-run
// creates, matching real API server behavior (the tracker would otherwise
// store the object and the real create would fail with AlreadyExists).
func honorCreateDryRun(dyn *dynfake.FakeDynamicClient) {
	dyn.PrependReactor("create", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := a.(k8stesting.CreateActionImpl)
		if !ok || len(ca.GetCreateOptions().DryRun) == 0 {
			return false, nil, nil
		}
		return true, ca.GetObject(), nil
	})
}

func TestApplyCreates(t *testing.T) {
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	honorCreateDryRun(dyn)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	e.HandleCommand(context.Background(), applyCommand(), rec.report)

	want := []string{protocol.StatusValidating, protocol.StatusDryRun, protocol.StatusApplying, protocol.StatusSucceeded}
	if got := rec.statuses(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("status sequence = %v, want %v", got, want)
	}

	live, err := dyn.Resource(cmGVR).Namespace("default").Get(context.Background(), "cm", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("object not created: %v", err)
	}
	if live.GetAnnotations()[drift.HashAnnotation] == "" {
		t.Fatal("drift hash annotation not stamped")
	}
	if live.GetAnnotations()[drift.HashAnnotation] != drift.HashObject(live) {
		t.Fatal("stamped hash does not match live object hash")
	}
}

func TestApplyDryRunPrecedesRealWrite(t *testing.T) {
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	honorCreateDryRun(dyn)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	e.HandleCommand(context.Background(), applyCommand(), rec.report)

	var creates []k8stesting.CreateActionImpl
	for _, a := range dyn.Actions() {
		if ca, ok := a.(k8stesting.CreateActionImpl); ok {
			creates = append(creates, ca)
		}
	}
	if len(creates) != 2 {
		t.Fatalf("expected 2 create actions (dry-run + real), got %d", len(creates))
	}
	if got := creates[0].GetCreateOptions().DryRun; len(got) != 1 || got[0] != metav1.DryRunAll {
		t.Fatalf("first create was not a dry-run: %v", got)
	}
	if got := creates[1].GetCreateOptions().DryRun; len(got) != 0 {
		t.Fatalf("second create should be the real write: %v", got)
	}
}

func liveCM(key string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]interface{}{"name": "cm", "namespace": "default"},
		"data":       map[string]interface{}{"key": key},
	}}
}

func TestApplyDriftPolicies(t *testing.T) {
	tests := []struct {
		name       string
		policy     string
		override   bool
		wantStatus string
	}{
		{"refuse", config.DriftRefuse, false, protocol.StatusRefused},
		{"overwrite", config.DriftOverwrite, false, protocol.StatusSucceeded},
		{"approval without override", config.DriftOverwriteWithApproval, false, protocol.StatusRefused},
		{"approval with override", config.DriftOverwriteWithApproval, true, protocol.StatusSucceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), liveCM("out-of-band-change"))
			cfg := testConfig()
			cfg.DriftPolicy = tt.policy
			e := newExecutor(cfg, dyn, kubefake.NewSimpleClientset())
			rec := &recorder{}

			cmd := applyCommand()
			cmd.ExpectedHash = "does-not-match-live"
			cmd.Override = tt.override
			e.HandleCommand(context.Background(), cmd, rec.report)

			if got := rec.last().Status; got != tt.wantStatus {
				t.Fatalf("last status = %q, want %q (sequence %v)", got, tt.wantStatus, rec.statuses())
			}
			if tt.wantStatus == protocol.StatusRefused && rec.last().Diff == "" {
				t.Fatal("refusal report should carry the drift diff")
			}
		})
	}
}

func TestDelete(t *testing.T) {
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), liveCM("x"))
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:     "cmd-del",
		Verb:   protocol.VerbDelete,
		Target: protocol.ResourceRef{Version: "v1", Resource: "configmaps", Namespace: "default", Name: "cm"},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("last status = %q, sequence %v", got, rec.statuses())
	}
	_, err := dyn.Resource(cmGVR).Namespace("default").Get(context.Background(), "cm", metav1.GetOptions{})
	if err == nil {
		t.Fatal("object still exists after delete")
	}
}

func TestPatchMerge(t *testing.T) {
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), liveCM("old"))
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:        "cmd-patch",
		Verb:      protocol.VerbPatch,
		PatchType: protocol.PatchMerge,
		Target:    protocol.ResourceRef{Version: "v1", Resource: "configmaps", Namespace: "default", Name: "cm"},
		Payload:   []byte(`{"data":{"key":"new"}}`),
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("last status = %q, sequence %v", got, rec.statuses())
	}
	live, err := dyn.Resource(cmGVR).Namespace("default").Get(context.Background(), "cm", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	key, _, _ := unstructured.NestedString(live.Object, "data", "key")
	if key != "new" {
		t.Fatalf("patch not applied, data.key = %q", key)
	}
	if live.GetAnnotations()[drift.HashAnnotation] != drift.HashObject(live) {
		t.Fatal("drift hash annotation not re-stamped after patch")
	}
	// dry-run patch + real patch + stamp patch.
	patches := 0
	for _, a := range dyn.Actions() {
		if a.GetVerb() == "patch" {
			patches++
		}
	}
	if patches != 3 {
		t.Fatalf("expected 3 patch actions, got %d", patches)
	}
}

func TestDrainNode(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	mkPod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       corev1.PodSpec{NodeName: "node-1"},
		}
	}
	dsPod := mkPod("ds-pod")
	dsPod.OwnerReferences = []metav1.OwnerReference{{Kind: "DaemonSet", APIVersion: "apps/v1", Name: "ds"}}
	mirrorPod := mkPod("mirror-pod")
	mirrorPod.Annotations = map[string]string{"kubernetes.io/config.mirror": "x"}
	// Pods in protected namespaces (e.g. the agent's own) must survive the
	// drain: evicting the agent cancels this very command via SIGTERM.
	protectedPod := mkPod("agent-pod")
	protectedPod.Namespace = "kube-system" // testConfig protects kube-system

	kube := kubefake.NewSimpleClientset(node, mkPod("p1"), mkPod("p2"), dsPod, mirrorPod, protectedPod)
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:     "cmd-drain",
		Verb:   protocol.VerbDrainNode,
		Target: protocol.ResourceRef{Version: "v1", Resource: "nodes", Name: "node-1"},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	statuses := rec.statuses()
	if got := statuses[len(statuses)-1]; got != protocol.StatusSucceeded {
		t.Fatalf("last status = %q, sequence %v", got, statuses)
	}

	// Cordon patch, pod list, two evictions.
	var cordon, evictions bool
	evictionCount := 0
	for _, a := range kube.Actions() {
		if a.GetVerb() == "patch" && a.GetResource().Resource == "nodes" {
			cordon = true
		}
		if a.GetVerb() == "create" && a.GetResource().Resource == "pods" && a.GetSubresource() == "eviction" {
			evictions = true
			evictionCount++
		}
	}
	if !cordon {
		t.Fatal("node was not cordoned")
	}
	if !evictions || evictionCount != 2 {
		t.Fatalf("expected 2 evictions, got %d", evictionCount)
	}

	n, err := kube.CoreV1().Nodes().Get(context.Background(), "node-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !n.Spec.Unschedulable {
		t.Fatal("node not marked unschedulable")
	}

	// Progress reports must reach 100.
	var sawProgress bool
	for _, rep := range rec.reports {
		if rep.Status == protocol.StatusProgress {
			sawProgress = true
		}
	}
	if !sawProgress {
		t.Fatal("no progress reports emitted")
	}
	if rec.reports[len(rec.reports)-2].Progress != 100 {
		t.Fatalf("penultimate report progress = %d, want 100", rec.reports[len(rec.reports)-2].Progress)
	}
}

func TestDrainNodeStall(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "stuck", Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: "node-1"},
	}
	kube := kubefake.NewSimpleClientset(node, pod)
	// Eviction keeps failing (simulating a PDB).
	kube.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() == "eviction" {
			return true, nil, &pdbError{}
		}
		return false, nil, nil
	})

	cfg := testConfig()
	cfg.DrainStallTimeout = 0 // stall immediately on first failure
	e := newExecutor(cfg, dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:     "cmd-drain-stall",
		Verb:   protocol.VerbDrainNode,
		Target: protocol.ResourceRef{Version: "v1", Resource: "nodes", Name: "node-1"},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	last := rec.last()
	if last.Status != protocol.StatusProgress || !strings.Contains(last.Message, "drain stalled") {
		t.Fatalf("expected stalled progress report, got %+v", last)
	}
	// Paused: no terminal succeeded/failed report.
	for _, rep := range rec.reports {
		if rep.Status == protocol.StatusSucceeded || rep.Status == protocol.StatusFailed {
			t.Fatalf("stalled drain must not reach a terminal state: %v", rec.statuses())
		}
	}
}

type pdbError struct{}

func (e *pdbError) Error() string { return "cannot evict pod: PDB budget exceeded" }

func TestUncordonNode(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Spec:       corev1.NodeSpec{Unschedulable: true},
	}
	kube := kubefake.NewSimpleClientset(node)
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kube)
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:     "cmd-uncordon",
		Verb:   protocol.VerbUncordonNode,
		Target: protocol.ResourceRef{Version: "v1", Resource: "nodes", Name: "node-1"},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("last status = %q, sequence %v", got, rec.statuses())
	}

	n, err := kube.CoreV1().Nodes().Get(context.Background(), "node-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if n.Spec.Unschedulable {
		t.Fatal("node still marked unschedulable after uncordon")
	}
}

func TestUncordonNodeRequiresName(t *testing.T) {
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kubefake.NewSimpleClientset())
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:     "cmd-uncordon-empty",
		Verb:   protocol.VerbUncordonNode,
		Target: protocol.ResourceRef{Version: "v1", Resource: "nodes"},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	if got := rec.last().Status; got != protocol.StatusFailed {
		t.Fatalf("last status = %q, sequence %v", got, rec.statuses())
	}
}

func TestCertRotate(t *testing.T) {
	certsGVR := schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
	crsGVR := schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificaterequests"}
	listKinds := map[schema.GroupVersionResource]string{
		certsGVR: "CertificateList",
		crsGVR:   "CertificateRequestList",
	}
	cert := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata":   map[string]interface{}{"name": "tls", "namespace": "apps"},
		"spec": map[string]interface{}{
			"issuerRef": map[string]interface{}{"name": "ca", "kind": "ClusterIssuer"},
			"usages":    []interface{}{"server auth"},
		},
	}}
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, cert)
	honorCreateDryRun(dyn)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:   "cmd-cert",
		Verb: protocol.VerbCertRotate,
		Target: protocol.ResourceRef{
			Group: "cert-manager.io", Version: "v1", Resource: "certificates",
			Namespace: "apps", Name: "tls",
		},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("last status = %q, sequence %v: %s", got, rec.statuses(), rec.last().Message)
	}

	list, err := dyn.Resource(crsGVR).Namespace("apps").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 CertificateRequest, got %d", len(list.Items))
	}
	cr := list.Items[0]
	if !strings.HasPrefix(cr.GetName(), "tls-renew-") {
		t.Fatalf("unexpected CertificateRequest name %q", cr.GetName())
	}
	if got := cr.GetAnnotations()["cert-manager.io/certificate-name"]; got != "tls" {
		t.Fatalf("certificate-name annotation = %q", got)
	}
	issuer, _, _ := unstructured.NestedMap(cr.Object, "spec", "issuerRef")
	if issuer["name"] != "ca" || issuer["kind"] != "ClusterIssuer" {
		t.Fatalf("issuerRef not copied: %v", issuer)
	}
	usages, _, _ := unstructured.NestedStringSlice(cr.Object, "spec", "usages")
	if len(usages) != 1 || usages[0] != "server auth" {
		t.Fatalf("usages not copied: %v", usages)
	}

	// Dry-run create must precede the real create.
	var creates []k8stesting.CreateActionImpl
	for _, a := range dyn.Actions() {
		if ca, ok := a.(k8stesting.CreateActionImpl); ok {
			creates = append(creates, ca)
		}
	}
	if len(creates) != 2 || len(creates[0].GetCreateOptions().DryRun) == 0 || len(creates[1].GetCreateOptions().DryRun) != 0 {
		t.Fatalf("expected dry-run create then real create, got %d creates", len(creates))
	}
}

func TestClusterUpgradeGuard(t *testing.T) {
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kubefake.NewSimpleClientset())
	rec := &recorder{}
	e.HandleCommand(context.Background(), &protocol.Command{ID: "x", Verb: protocol.VerbClusterUpgrade}, rec.report)
	if got := rec.last().Status; got != protocol.StatusRefused {
		t.Fatalf("cluster-upgrade must be refused, got %q", got)
	}
}

func TestUnknownVerbRefused(t *testing.T) {
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kubefake.NewSimpleClientset())
	rec := &recorder{}
	e.HandleCommand(context.Background(), &protocol.Command{ID: "x", Verb: "self-destruct"}, rec.report)
	if got := rec.last().Status; got != protocol.StatusRefused {
		t.Fatalf("unknown verb must be refused, got %q", got)
	}
}

var (
	secretGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
	deployGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	nsGVR     = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
)

// applyCommandEmptyTarget mirrors what the control plane sends for s2s
// manifest deployments: an apply whose target is entirely empty — the
// payload is the whole command.
func applyCommandEmptyTarget(payload string) *protocol.Command {
	return &protocol.Command{
		ID:      "cmd-s2s",
		Verb:    protocol.VerbApply,
		Payload: []byte(payload),
	}
}

// The payload carries two documents and the command target is empty: both
// objects must be created, with the GVR resolved from each document's own
// apiVersion/kind. Regression test for s2s deployments failing with
// `cannot get resource "<namespace>" in API group ""` (the namespace value
// landed in the resource position because the GVR came from the empty
// target).
func TestApplyMultiDocManifestWithEmptyTarget(t *testing.T) {
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	honorCreateDryRun(dyn)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	payload := `apiVersion: v1
kind: Secret
metadata:
  name: pull-secret
  namespace: apps
type: kubernetes.io/dockerconfigjson
data:
  key: dmFsdWU=
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: apps
spec:
  replicas: 1
`
	e.HandleCommand(context.Background(), applyCommandEmptyTarget(payload), rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("multi-doc apply failed: %q (%s)", got, rec.last().Message)
	}
	if _, err := dyn.Resource(secretGVR).Namespace("apps").Get(context.Background(), "pull-secret", metav1.GetOptions{}); err != nil {
		t.Fatalf("secret not created: %v", err)
	}
	if _, err := dyn.Resource(deployGVR).Namespace("apps").Get(context.Background(), "web", metav1.GetOptions{}); err != nil {
		t.Fatalf("deployment not created: %v", err)
	}
	if !strings.Contains(rec.last().Message, "2 created") {
		t.Fatalf("success message %q should report 2 created objects", rec.last().Message)
	}
}

// Cluster-scoped kinds must be applied without a namespace.
func TestApplyClusterScopedObject(t *testing.T) {
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	honorCreateDryRun(dyn)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	payload := "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: team-a\n"
	e.HandleCommand(context.Background(), applyCommandEmptyTarget(payload), rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("cluster-scoped apply failed: %q (%s)", got, rec.last().Message)
	}
	if _, err := dyn.Resource(nsGVR).Get(context.Background(), "team-a", metav1.GetOptions{}); err != nil {
		t.Fatalf("namespace not created: %v", err)
	}
}

// A payload kind unknown to discovery with an empty target must fail with a
// clear message instead of hitting the API server with an empty resource.
func TestApplyUnknownKindWithEmptyTargetFails(t *testing.T) {
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kubefake.NewSimpleClientset())
	rec := &recorder{}

	payload := "apiVersion: example.io/v1\nkind: Bogus\nmetadata:\n  name: x\n  namespace: default\n"
	e.HandleCommand(context.Background(), applyCommandEmptyTarget(payload), rec.report)

	if got := rec.last().Status; got != protocol.StatusFailed {
		t.Fatalf("unknown kind must fail, got %q", got)
	}
	if !strings.Contains(rec.last().Message, "cannot resolve resource type") {
		t.Fatalf("unexpected failure message: %q", rec.last().Message)
	}
}

// A kind: List payload is expanded and every item applied.
func TestApplyKindList(t *testing.T) {
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme())
	honorCreateDryRun(dyn)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	payload := `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata:
    name: one
    namespace: default
- apiVersion: v1
  kind: ConfigMap
  metadata:
    name: two
    namespace: default
`
	e.HandleCommand(context.Background(), applyCommandEmptyTarget(payload), rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("List apply failed: %q (%s)", got, rec.last().Message)
	}
	for _, name := range []string{"one", "two"} {
		if _, err := dyn.Resource(cmGVR).Namespace("default").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
			t.Fatalf("configmap %s not created: %v", name, err)
		}
	}
}

// Applying over an already-bound PVC must adopt the volume binding and
// clamp the request up to the live capacity — the exact failure a blind
// full replace produced on redeploy.
func TestApplyUpdatesExistingPVC(t *testing.T) {
	live := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata":   map[string]interface{}{"name": "data", "namespace": "default", "resourceVersion": "7"},
		"spec": map[string]interface{}{
			"storageClassName": "hcloud-volumes",
			"volumeName":       "pvc-123",
			"accessModes":      []interface{}{"ReadWriteOnce"},
			"resources": map[string]interface{}{
				"requests": map[string]interface{}{"storage": "10Gi"},
			},
		},
		"status": map[string]interface{}{
			"capacity": map[string]interface{}{"storage": "20Gi"},
		},
	}}
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), live)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	payload := `apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: default
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
`
	e.HandleCommand(context.Background(), applyCommandEmptyTarget(payload), rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("apply over bound PVC failed: %q (%s)", got, rec.last().Message)
	}
	if !strings.Contains(rec.last().Message, "1 updated") {
		t.Fatalf("expected an update, got %q", rec.last().Message)
	}
	got, err := dyn.Resource(pvcGVR).Namespace("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cannot fetch pvc: %v", err)
	}
	if v, _, _ := unstructured.NestedString(got.Object, "spec", "volumeName"); v != "pvc-123" {
		t.Fatalf("volumeName not adopted from live: %q", v)
	}
	if v, _, _ := unstructured.NestedString(got.Object, "spec", "storageClassName"); v != "hcloud-volumes" {
		t.Fatalf("storageClassName not adopted from live: %q", v)
	}
	if v, _, _ := unstructured.NestedString(got.Object, "spec", "resources", "requests", "storage"); v != "20Gi" {
		t.Fatalf("storage request not clamped to live capacity: %q", v)
	}
}

// Applying over an existing Deployment whose manifest carries a changed
// selector must adopt the live (immutable) selector and reconcile the pod
// template labels with it, instead of failing on "field is immutable".
func TestApplyUpdateAdoptsDeploymentSelector(t *testing.T) {
	live := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]interface{}{"name": "logger", "namespace": "default", "resourceVersion": "7"},
		"spec": map[string]interface{}{
			"selector": map[string]interface{}{
				"matchLabels": map[string]interface{}{"app": "logger"},
			},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{"app": "logger"},
				},
			},
		},
	}}
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), live)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	payload := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: logger
  namespace: default
spec:
  selector:
    matchLabels:
      app: logger
      Octopus.Kubernetes.DeploymentName: logger
  template:
    metadata:
      labels:
        app: logger
        Octopus.Kubernetes.DeploymentName: logger
        version: "2"
`
	e.HandleCommand(context.Background(), applyCommandEmptyTarget(payload), rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("apply over existing Deployment failed: %q (%s)", got, rec.last().Message)
	}
	got, err := dyn.Resource(deployGVR).Namespace("default").Get(context.Background(), "logger", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cannot fetch deployment: %v", err)
	}
	selector, _, _ := unstructured.NestedStringMap(got.Object, "spec", "selector", "matchLabels")
	if len(selector) != 1 || selector["app"] != "logger" {
		t.Fatalf("selector not adopted from live: %v", selector)
	}
	labels, _, _ := unstructured.NestedStringMap(got.Object, "spec", "template", "metadata", "labels")
	if labels["app"] != "logger" {
		t.Fatalf("template label for selector key lost: %v", labels)
	}
	if labels["version"] != "2" {
		t.Fatalf("manifest template label not applied: %v", labels)
	}
}

// An update keeps live state the manifest does not mention: labels, extra
// data keys and server-set fields survive a redeploy.
func TestApplyUpdatePreservesUnspecifiedLiveFields(t *testing.T) {
	live := liveCM("old")
	live.Object["data"] = map[string]interface{}{"key": "old", "extra": "keep"}
	live.SetLabels(map[string]string{"deployed-by": "agent"})
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), live)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	e.HandleCommand(context.Background(), applyCommand(), rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("update failed: %q (%s)", got, rec.last().Message)
	}
	if !strings.Contains(rec.last().Message, "1 updated") {
		t.Fatalf("expected an update, got %q", rec.last().Message)
	}
	got, err := dyn.Resource(cmGVR).Namespace("default").Get(context.Background(), "cm", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cannot fetch configmap: %v", err)
	}
	data, _, _ := unstructured.NestedStringMap(got.Object, "data")
	if data["key"] != "desired" {
		t.Fatalf("manifest field not applied: data.key = %q", data["key"])
	}
	if data["extra"] != "keep" {
		t.Fatalf("live-only field lost: data.extra = %q", data["extra"])
	}
	if got.GetLabels()["deployed-by"] != "agent" {
		t.Fatal("live-only label lost")
	}
}

// Applying over an existing Service adopts the allocated clusterIP fields.
func TestApplyUpdateAdoptsServiceClusterIP(t *testing.T) {
	live := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]interface{}{"name": "web", "namespace": "default", "resourceVersion": "3"},
		"spec": map[string]interface{}{
			"clusterIP":      "10.0.0.5",
			"clusterIPs":     []interface{}{"10.0.0.5"},
			"ipFamilies":     []interface{}{"IPv4"},
			"ipFamilyPolicy": "SingleStack",
			"ports": []interface{}{map[string]interface{}{
				"port": int64(80), "targetPort": int64(8080),
			}},
		},
	}}
	dyn := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), live)
	e := newExecutor(testConfig(), dyn, kubefake.NewSimpleClientset())
	rec := &recorder{}

	payload := `apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: default
spec:
  ports:
  - port: 80
    targetPort: 8080
`
	e.HandleCommand(context.Background(), applyCommandEmptyTarget(payload), rec.report)

	if got := rec.last().Status; got != protocol.StatusSucceeded {
		t.Fatalf("apply over existing Service failed: %q (%s)", got, rec.last().Message)
	}
	got, err := dyn.Resource(svcGVR).Namespace("default").Get(context.Background(), "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("cannot fetch service: %v", err)
	}
	if v, _, _ := unstructured.NestedString(got.Object, "spec", "clusterIP"); v != "10.0.0.5" {
		t.Fatalf("clusterIP not adopted from live: %q", v)
	}
}
