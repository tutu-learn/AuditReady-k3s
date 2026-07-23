package executor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/registry"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"helm.sh/helm/v3/pkg/chart"

	"AuditReady-k3s/internal/protocol"
)

// restClientGetter adapts an injected *rest.Config to Helm's
// genericclioptions.RESTClientGetter so Helm talks to the same cluster as
// the rest of the agent.
type restClientGetter struct {
	restCfg *rest.Config

	discOnce sync.Once
	disc     discovery.CachedDiscoveryInterface
	discErr  error

	mapperOnce sync.Once
	mapper     meta.RESTMapper
	mapperErr  error
}

func (g *restClientGetter) ToRESTConfig() (*rest.Config, error) { return g.restCfg, nil }

func (g *restClientGetter) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	g.discOnce.Do(func() {
		dc, err := discovery.NewDiscoveryClientForConfig(g.restCfg)
		if err != nil {
			g.discErr = err
			return
		}
		g.disc = memory.NewMemCacheClient(dc)
	})
	return g.disc, g.discErr
}

func (g *restClientGetter) ToRESTMapper() (meta.RESTMapper, error) {
	g.mapperOnce.Do(func() {
		disc, err := g.ToDiscoveryClient()
		if err != nil {
			g.mapperErr = err
			return
		}
		g.mapper = restmapper.NewDeferredDiscoveryRESTMapper(disc)
	})
	return g.mapper, g.mapperErr
}

func (g *restClientGetter) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	// No kubeconfig file: the in-cluster rest.Config is authoritative.
	return clientcmd.NewDefaultClientConfig(*clientcmdapi.NewConfig(), &clientcmd.ConfigOverrides{})
}

var _ genericclioptions.RESTClientGetter = (*restClientGetter)(nil)

// helmConfig builds an action.Configuration bound to the agent's
// rest.Config, storing release state in Secrets.
func (e *Executor) helmConfig(namespace string) (*action.Configuration, error) {
	cfg := new(action.Configuration)
	err := cfg.Init(&restClientGetter{restCfg: e.restCfg}, namespace, "secrets", func(format string, args ...interface{}) {
		e.log.Debug(fmt.Sprintf(format, args...))
	})
	if err != nil {
		return nil, err
	}
	if cfg.RegistryClient == nil {
		rc, err := registry.NewClient()
		if err != nil {
			return nil, err
		}
		cfg.RegistryClient = rc
	}
	return cfg, nil
}

// parseValues parses the optional values YAML. It runs before any chart
// lookup so malformed values fail fast without cluster access.
func parseValues(spec *protocol.HelmSpec) (map[string]interface{}, error) {
	if len(spec.ValuesYAML) == 0 {
		return map[string]interface{}{}, nil
	}
	vals, err := chartutil.ReadValues(spec.ValuesYAML)
	if err != nil {
		return nil, fmt.Errorf("cannot parse values YAML: %w", err)
	}
	return vals, nil
}

// locateChart resolves ChartRef to a loaded chart: local path, OCI ref, or
// repo chart ("repo/chart" or URL) with optional version.
func locateChart(spec *protocol.HelmSpec, cpo *action.ChartPathOptions) (*chart.Chart, error) {
	ref := spec.ChartRef
	if _, err := os.Stat(ref); err == nil || strings.HasPrefix(ref, ".") || strings.HasPrefix(ref, "/") {
		return loader.Load(ref)
	}
	cpo.Version = spec.Version
	path, err := cpo.LocateChart(ref, cli.New())
	if err != nil {
		return nil, err
	}
	return loader.Load(path)
}

func (e *Executor) helmInstall(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	h := cmd.Helm
	vals, err := parseValues(h)
	if err != nil {
		e.fail(cmd, report, log, "helm-install", err)
		return
	}
	cfg, err := e.helmConfig(h.Namespace)
	if err != nil {
		e.fail(cmd, report, log, "helm-install", err)
		return
	}

	newInstall := func() *action.Install {
		inst := action.NewInstall(cfg)
		inst.ReleaseName = h.ReleaseName
		inst.Namespace = h.Namespace
		inst.CreateNamespace = false
		return inst
	}

	chrt, err := locateChart(h, &newInstall().ChartPathOptions)
	if err != nil {
		e.fail(cmd, report, log, "cannot locate chart", err)
		return
	}

	if e.cfg.DryRunFirst {
		inst := newInstall()
		inst.DryRun = true
		if _, err := inst.RunWithContext(ctx, chrt, vals); err != nil {
			e.fail(cmd, report, log, "helm-install dry-run", sanitizeHelmErr(err))
			return
		}
		report(&protocol.Report{Status: protocol.StatusDryRun, Message: fmt.Sprintf("dry-run install of release %q in %s validated", h.ReleaseName, h.Namespace)})
	}

	report(&protocol.Report{Status: protocol.StatusApplying, Message: "helm install " + h.ReleaseName})
	if _, err := newInstall().RunWithContext(ctx, chrt, vals); err != nil {
		e.fail(cmd, report, log, "helm-install", sanitizeHelmErr(err))
		return
	}
	e.succeed(cmd, report, fmt.Sprintf("installed release %q in %s", h.ReleaseName, h.Namespace))
}

func (e *Executor) helmUpgrade(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	h := cmd.Helm
	vals, err := parseValues(h)
	if err != nil {
		e.fail(cmd, report, log, "helm-upgrade", err)
		return
	}
	cfg, err := e.helmConfig(h.Namespace)
	if err != nil {
		e.fail(cmd, report, log, "helm-upgrade", err)
		return
	}

	// Refuse to upgrade a release that does not exist.
	if _, err := action.NewStatus(cfg).Run(h.ReleaseName); err != nil {
		e.fail(cmd, report, log, "helm-upgrade", fmt.Errorf("release %q not found in namespace %q", h.ReleaseName, h.Namespace))
		return
	}

	newUpgrade := func() *action.Upgrade {
		up := action.NewUpgrade(cfg)
		up.Namespace = h.Namespace
		return up
	}

	chrt, err := locateChart(h, &newUpgrade().ChartPathOptions)
	if err != nil {
		e.fail(cmd, report, log, "cannot locate chart", err)
		return
	}

	if e.cfg.DryRunFirst {
		up := newUpgrade()
		up.DryRun = true
		if _, err := up.RunWithContext(ctx, h.ReleaseName, chrt, vals); err != nil {
			e.fail(cmd, report, log, "helm-upgrade dry-run", sanitizeHelmErr(err))
			return
		}
		report(&protocol.Report{Status: protocol.StatusDryRun, Message: fmt.Sprintf("dry-run upgrade of release %q in %s validated", h.ReleaseName, h.Namespace)})
	}

	report(&protocol.Report{Status: protocol.StatusApplying, Message: "helm upgrade " + h.ReleaseName})
	if _, err := newUpgrade().RunWithContext(ctx, h.ReleaseName, chrt, vals); err != nil {
		e.fail(cmd, report, log, "helm-upgrade", sanitizeHelmErr(err))
		return
	}
	e.succeed(cmd, report, fmt.Sprintf("upgraded release %q in %s", h.ReleaseName, h.Namespace))
}

func (e *Executor) helmUninstall(ctx context.Context, cmd *protocol.Command, report func(*protocol.Report), log *slog.Logger) {
	h := cmd.Helm
	cfg, err := e.helmConfig(h.Namespace)
	if err != nil {
		e.fail(cmd, report, log, "helm-uninstall", err)
		return
	}

	// Helm has no uninstall dry-run: execute directly.
	report(&protocol.Report{Status: protocol.StatusApplying, Message: "helm uninstall " + h.ReleaseName})
	if _, err := action.NewUninstall(cfg).Run(h.ReleaseName); err != nil {
		e.fail(cmd, report, log, "helm-uninstall", sanitizeHelmErr(err))
		return
	}
	e.succeed(cmd, report, fmt.Sprintf("uninstalled release %q from %s", h.ReleaseName, h.Namespace))
}

// sanitizeHelmErr trims Helm errors to a single line; rendered manifests or
// values must never leak into reports.
func sanitizeHelmErr(err error) error {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return fmt.Errorf("%s", msg)
}
