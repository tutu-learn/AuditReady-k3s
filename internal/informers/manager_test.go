package informers

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	fakeDiscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	"AuditReady-k3s/internal/protocol"
)

// secretValueB64 is the base64 of "supersecret"; it must never leave the cluster.
const secretValueB64 = "c3VwZXJzZWNyZXQ="

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type recordSink struct {
	mu     sync.Mutex
	events []*protocol.InventoryEvent
}

func (s *recordSink) Enqueue(ev *protocol.InventoryEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
}

func (s *recordSink) all() []*protocol.InventoryEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*protocol.InventoryEvent(nil), s.events...)
}

func newObj(apiVersion, kind, ns, name string, extra map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": ns,
		},
	}
	for k, v := range extra {
		obj[k] = v
	}
	return &unstructured.Unstructured{Object: obj}
}

func deployment() *unstructured.Unstructured {
	return newObj("apps/v1", "Deployment", "default", "web", map[string]any{"replicas": int64(2)})
}

func secret() *unstructured.Unstructured {
	return newObj("v1", "Secret", "default", "db", map[string]any{
		"type": "Opaque",
		"data": map[string]any{"password": secretValueB64, "username": "YWRtaW4="},
	})
}

func configMap() *unstructured.Unstructured {
	return newObj("v1", "ConfigMap", "default", "settings", map[string]any{
		"data": map[string]any{"key": "value"},
	})
}

// fakeDisco serves core/v1 and apps/v1 only — no cert-manager CRDs.
func fakeDisco() *fakeDiscovery.FakeDiscovery {
	return &fakeDiscovery.FakeDiscovery{
		Fake: &clienttesting.Fake{
			Resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "secrets", Kind: "Secret", Namespaced: true},
						{Name: "configmaps", Kind: "ConfigMap", Namespaced: true},
					},
				},
				{
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{Name: "deployments", Kind: "Deployment", Namespaced: true},
					},
				},
			},
		},
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func findEvent(events []*protocol.InventoryEvent, resource, name string) *protocol.InventoryEvent {
	for _, ev := range events {
		if ev.Ref.Resource == resource && ev.Ref.Name == name {
			return ev
		}
	}
	return nil
}

func TestResolve(t *testing.T) {
	t.Run("all known names", func(t *testing.T) {
		got, err := Resolve([]string{
			"deployments", "statefulsets", "daemonsets", "replicasets",
			"pods", "services", "configmaps", "secrets", "nodes", "namespaces",
			"persistentvolumeclaims", "serviceaccounts",
			"ingresses", "networkpolicies",
			"roles", "rolebindings", "clusterroles", "clusterrolebindings",
			"poddisruptionbudgets",
			"certificates", "issuers", "clusterissuers",
		})
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if len(got) != 22 {
			t.Fatalf("expected 22 resources, got %d", len(got))
		}
		byResource := map[string]WatchedResource{}
		for _, w := range got {
			byResource[w.GVR.Resource] = w
		}
		if w := byResource["secrets"]; !w.Secret || !w.Namespaced {
			t.Errorf("secrets: expected Secret and Namespaced, got %+v", w)
		}
		if w := byResource["clusterissuers"]; w.Namespaced || w.GVR.Group != "cert-manager.io" {
			t.Errorf("clusterissuers: unexpected %+v", w)
		}
		if w := byResource["nodes"]; w.Namespaced || w.GVR.Group != "" {
			t.Errorf("nodes: unexpected %+v", w)
		}
	})
	t.Run("unknown name lists known", func(t *testing.T) {
		_, err := Resolve([]string{"bogus"})
		if err == nil {
			t.Fatal("expected error")
		}
		if got := err.Error(); !contains(got, "bogus") || !contains(got, "deployments") {
			t.Errorf("error should name the unknown and list known names, got %q", got)
		}
	})
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestManagerEventsAndSnapshot(t *testing.T) {
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClient(scheme, deployment(), secret(), configMap())
	resources, err := Resolve([]string{"deployments", "secrets", "configmaps"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	sink := &recordSink{}
	m := New(client, fakeDisco(), resources, sink, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	waitFor(t, "cache sync", m.HasSynced)
	waitFor(t, "initial add events", func() bool { return len(sink.all()) >= 3 })

	events := sink.all()

	dep := findEvent(events, "deployments", "web")
	if dep == nil {
		t.Fatal("no event for deployment web")
	}
	if dep.Op != protocol.OpAdd {
		t.Errorf("deployment op = %q, want add", dep.Op)
	}
	wantRef := protocol.ResourceRef{Group: "apps", Version: "v1", Resource: "deployments", Namespace: "default", Name: "web"}
	if dep.Ref != wantRef {
		t.Errorf("deployment ref = %+v, want %+v", dep.Ref, wantRef)
	}
	if dep.Timestamp == 0 {
		t.Error("deployment event has no timestamp")
	}

	sec := findEvent(events, "secrets", "db")
	if sec == nil {
		t.Fatal("no event for secret db")
	}
	if contains(string(sec.ObjectJSON), secretValueB64) || contains(string(sec.ObjectJSON), "YWRtaW4=") {
		t.Errorf("secret event leaks data values: %s", sec.ObjectJSON)
	}
	if !contains(string(sec.ObjectJSON), `"password"`) || !contains(string(sec.ObjectJSON), `"username"`) {
		t.Errorf("secret event must keep data key names: %s", sec.ObjectJSON)
	}
	if !contains(string(sec.ObjectJSON), `"Opaque"`) {
		t.Errorf("secret event must keep the type: %s", sec.ObjectJSON)
	}

	// Snapshot carries every object as OpSync, secrets still stripped.
	snap := m.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot has %d events, want 3", len(snap))
	}
	for _, ev := range snap {
		if ev.Op != protocol.OpSync {
			t.Errorf("snapshot event op = %q, want sync", ev.Op)
		}
	}
	snapSec := findEvent(snap, "secrets", "db")
	if snapSec == nil {
		t.Fatal("snapshot missing secret db")
	}
	if contains(string(snapSec.ObjectJSON), secretValueB64) {
		t.Errorf("snapshot secret leaks data values: %s", snapSec.ObjectJSON)
	}
}

func TestManagerSkipsMissingCRDs(t *testing.T) {
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClient(scheme, deployment())
	// certificates is a cert-manager CRD; fakeDisco does not serve it.
	resources, err := Resolve([]string{"deployments", "certificates"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	sink := &recordSink{}
	m := New(client, fakeDisco(), resources, sink, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	// The deployments informer must sync despite certificates being absent.
	waitFor(t, "cache sync", m.HasSynced)
	waitFor(t, "deployment event", func() bool {
		return findEvent(sink.all(), "deployments", "web") != nil
	})
	for _, ev := range m.Snapshot() {
		if ev.Ref.Group == "cert-manager.io" {
			t.Errorf("unexpected cert-manager event %+v", ev.Ref)
		}
	}
}
