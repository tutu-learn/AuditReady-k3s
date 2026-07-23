package informers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"AuditReady-k3s/internal/metrics"
	"AuditReady-k3s/internal/protocol"
)

// lagReportInterval is how often per-resource cache sync lag is reported.
const lagReportInterval = 30 * time.Second

// Sink receives inventory events from informers.
type Sink interface {
	Enqueue(ev *protocol.InventoryEvent)
}

// Manager runs one shared informer per WatchedResource and forwards events
// to the sink. It is read-only: it only lists/watches through the dynamic
// client it is given.
type Manager struct {
	sink Sink
	log  *slog.Logger

	factory   dynamicinformer.DynamicSharedInformerFactory
	informers map[schema.GroupVersionResource]informers.GenericInformer
	secrets   map[schema.GroupVersionResource]bool

	mu        sync.Mutex
	lastEvent map[string]time.Time // metric label -> last event time
}

// New builds a Manager watching the given resources. Resources whose GVR is
// not served by the cluster (e.g. cert-manager CRDs not installed) are
// skipped with a warning.
func New(reader dynamic.Interface, disco discovery.DiscoveryInterface, resources []WatchedResource, sink Sink, log *slog.Logger) *Manager {
	m := &Manager{
		sink:      sink,
		log:       log,
		informers: make(map[schema.GroupVersionResource]informers.GenericInformer),
		secrets:   make(map[schema.GroupVersionResource]bool),
		lastEvent: make(map[string]time.Time),
	}
	m.factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(reader, 0, metav1.NamespaceAll, nil)
	for _, w := range resources {
		if !m.served(disco, w) {
			continue
		}
		inf := m.factory.ForResource(w.GVR)
		handler := cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { m.handle(protocol.OpAdd, w, obj) },
			UpdateFunc: func(_, obj any) { m.handle(protocol.OpUpdate, w, obj) },
			DeleteFunc: func(obj any) { m.handle(protocol.OpDelete, w, obj) },
		}
		if _, err := inf.Informer().AddEventHandler(handler); err != nil {
			m.log.Error("add event handler", "resource", w.GVR.String(), "error", err)
			continue
		}
		m.informers[w.GVR] = inf
		m.secrets[w.GVR] = w.Secret
		m.lastEvent[w.GVR.Resource] = time.Now()
	}
	return m
}

// served reports whether the cluster serves the resource's GVR.
func (m *Manager) served(disco discovery.DiscoveryInterface, w WatchedResource) bool {
	list, err := disco.ServerResourcesForGroupVersion(w.GVR.GroupVersion().String())
	if err != nil {
		m.log.Warn("discovery failed, skipping resource", "resource", w.GVR.String(), "error", err)
		return false
	}
	for _, r := range list.APIResources {
		if r.Name == w.GVR.Resource {
			return true
		}
	}
	m.log.Warn("resource not served by cluster, skipping", "resource", w.GVR.String())
	return false
}

// Run starts the informers, waits for the initial cache sync, then reports
// cache sync lag until ctx is done.
func (m *Manager) Run(ctx context.Context) {
	if len(m.informers) == 0 {
		m.log.Warn("no informers to run")
		<-ctx.Done()
		return
	}
	m.factory.Start(ctx.Done())
	syncs := make([]cache.InformerSynced, 0, len(m.informers))
	for _, inf := range m.informers {
		syncs = append(syncs, inf.Informer().HasSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), syncs...) {
		return
	}
	m.log.Info("informer caches synced", "count", len(syncs))
	ticker := time.NewTicker(lagReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.observeLag()
		}
	}
}

// HasSynced reports whether every started informer has synced at least once.
func (m *Manager) HasSynced() bool {
	if len(m.informers) == 0 {
		return false
	}
	for _, inf := range m.informers {
		if !inf.Informer().HasSynced() {
			return false
		}
	}
	return true
}

// Snapshot emits an OpSync event for every object in every informer cache.
func (m *Manager) Snapshot() []*protocol.InventoryEvent {
	var out []*protocol.InventoryEvent
	for gvr, inf := range m.informers {
		for _, obj := range inf.Informer().GetStore().List() {
			ev, err := toEvent(protocol.OpSync, gvr, obj, m.secrets[gvr])
			if err != nil {
				m.log.Error("snapshot event", "resource", gvr.String(), "error", err)
				continue
			}
			out = append(out, ev)
		}
	}
	return out
}

// handle converts an informer notification into an inventory event.
func (m *Manager) handle(op string, w WatchedResource, obj any) {
	ev, err := toEvent(op, w.GVR, obj, w.Secret)
	if err != nil {
		m.log.Error("convert object", "resource", w.GVR.String(), "op", op, "error", err)
		return
	}
	m.mu.Lock()
	m.lastEvent[w.GVR.Resource] = time.Now()
	m.mu.Unlock()
	m.sink.Enqueue(ev)
}

func (m *Manager) observeLag() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for resource, last := range m.lastEvent {
		metrics.ObserveCacheSyncLag(resource, now.Sub(last).Seconds())
	}
}

// toEvent builds an inventory event from an informer object, unpacking
// DeletedFinalStateUnknown tombstones and stripping secret values.
func toEvent(op string, gvr schema.GroupVersionResource, obj any, secret bool) (*protocol.InventoryEvent, error) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unexpected object type %T", obj)
	}
	ev := &protocol.InventoryEvent{
		Op: op,
		Ref: protocol.ResourceRef{
			Group:     gvr.Group,
			Version:   gvr.Version,
			Resource:  gvr.Resource,
			Namespace: u.GetNamespace(),
			Name:      u.GetName(),
		},
		Timestamp: time.Now().Unix(),
	}
	payload := u.Object
	if secret {
		payload = stripSecret(u)
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	ev.ObjectJSON = b
	return ev, nil
}

// stripSecret returns a copy of a Secret object carrying only metadata, the
// type, and the set of data keys — never their values.
func stripSecret(u *unstructured.Unstructured) map[string]any {
	keys := map[string]string{}
	if data, found, err := unstructured.NestedStringMap(u.Object, "data"); err == nil && found {
		for k := range data {
			keys[k] = ""
		}
	} else if raw, found, _ := unstructured.NestedMap(u.Object, "data"); found {
		for k := range raw {
			keys[k] = ""
		}
	}
	typ, _, _ := unstructured.NestedString(u.Object, "type")
	return map[string]any{
		"apiVersion": u.GetAPIVersion(),
		"kind":       u.GetKind(),
		"metadata": map[string]any{
			"name":              u.GetName(),
			"namespace":         u.GetNamespace(),
			"labels":            u.GetLabels(),
			"annotations":       u.GetAnnotations(),
			"creationTimestamp": u.GetCreationTimestamp(),
		},
		"type": typ,
		"data": keys,
	}
}
