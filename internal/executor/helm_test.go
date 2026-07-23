package executor

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	dynfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"AuditReady-k3s/internal/protocol"
)

// NOTE: helm install/upgrade/uninstall are NOT exercised against a real
// cluster here. These tests only cover the RESTClientGetter wiring and
// error surfacing for malformed values.

func TestRESTClientGetterReturnsInjectedConfig(t *testing.T) {
	rc := &rest.Config{Host: "https://127.0.0.1:6443", BearerToken: "x"}
	g := &restClientGetter{restCfg: rc}

	got, err := g.ToRESTConfig()
	if err != nil {
		t.Fatal(err)
	}
	if got != rc {
		t.Fatalf("ToRESTConfig = %p, want injected %p", got, rc)
	}
	if loader := g.ToRawKubeConfigLoader(); loader == nil {
		t.Fatal("ToRawKubeConfigLoader returned nil")
	}
}

func TestHelmInstallMalformedValuesFails(t *testing.T) {
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kubefake.NewSimpleClientset())
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:   "cmd-helm",
		Verb: protocol.VerbHelmInstall,
		Helm: &protocol.HelmSpec{
			ReleaseName: "app",
			Namespace:   "apps",
			ChartRef:    "./does-not-need-to-exist", // values parsing fails first
			ValuesYAML:  []byte("a: [unclosed"),
		},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	last := rec.last()
	if last.Status != protocol.StatusFailed {
		t.Fatalf("last status = %q, want failed (sequence %v)", last.Status, rec.statuses())
	}
	if !strings.Contains(last.Message, "values") {
		t.Fatalf("failure message should mention values parsing: %q", last.Message)
	}
}

func TestHelmUpgradeMalformedValuesFails(t *testing.T) {
	e := newExecutor(testConfig(), dynfake.NewSimpleDynamicClient(runtime.NewScheme()), kubefake.NewSimpleClientset())
	rec := &recorder{}

	cmd := &protocol.Command{
		ID:   "cmd-helm-up",
		Verb: protocol.VerbHelmUpgrade,
		Helm: &protocol.HelmSpec{
			ReleaseName: "app",
			Namespace:   "apps",
			ChartRef:    "repo/chart",
			ValuesYAML:  []byte("\tbad indent"),
		},
	}
	e.HandleCommand(context.Background(), cmd, rec.report)

	if got := rec.last().Status; got != protocol.StatusFailed {
		t.Fatalf("last status = %q, want failed (sequence %v)", got, rec.statuses())
	}
}
