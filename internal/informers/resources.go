// Package informers watches configured Kubernetes resources through shared
// informers backed by a read-only dynamic client, and forwards inventory
// events to a Sink. The watch path never writes to the API server.
package informers

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// WatchedResource describes a single resource type to watch.
type WatchedResource struct {
	GVR        schema.GroupVersionResource
	Namespaced bool
	Secret     bool // values must be stripped before leaving the cluster
}

// known maps user-facing kind names (as used in config.DefaultInformers) to
// their watched resources.
var known = map[string]WatchedResource{
	// apps/v1
	"deployments":  {GVR: gvr("apps", "v1", "deployments"), Namespaced: true},
	"statefulsets": {GVR: gvr("apps", "v1", "statefulsets"), Namespaced: true},
	"daemonsets":   {GVR: gvr("apps", "v1", "daemonsets"), Namespaced: true},
	"replicasets":  {GVR: gvr("apps", "v1", "replicasets"), Namespaced: true},
	// core/v1
	"pods":                   {GVR: gvr("", "v1", "pods"), Namespaced: true},
	"services":               {GVR: gvr("", "v1", "services"), Namespaced: true},
	"configmaps":             {GVR: gvr("", "v1", "configmaps"), Namespaced: true},
	"secrets":                {GVR: gvr("", "v1", "secrets"), Namespaced: true, Secret: true},
	"nodes":                  {GVR: gvr("", "v1", "nodes")},
	"namespaces":             {GVR: gvr("", "v1", "namespaces")},
	"persistentvolumeclaims": {GVR: gvr("", "v1", "persistentvolumeclaims"), Namespaced: true},
	"serviceaccounts":        {GVR: gvr("", "v1", "serviceaccounts"), Namespaced: true},
	// networking.k8s.io/v1
	"ingresses":       {GVR: gvr("networking.k8s.io", "v1", "ingresses"), Namespaced: true},
	"networkpolicies": {GVR: gvr("networking.k8s.io", "v1", "networkpolicies"), Namespaced: true},
	// rbac.authorization.k8s.io/v1
	"roles":               {GVR: gvr("rbac.authorization.k8s.io", "v1", "roles"), Namespaced: true},
	"rolebindings":        {GVR: gvr("rbac.authorization.k8s.io", "v1", "rolebindings"), Namespaced: true},
	"clusterroles":        {GVR: gvr("rbac.authorization.k8s.io", "v1", "clusterroles")},
	"clusterrolebindings": {GVR: gvr("rbac.authorization.k8s.io", "v1", "clusterrolebindings")},
	// policy/v1
	"poddisruptionbudgets": {GVR: gvr("policy", "v1", "poddisruptionbudgets"), Namespaced: true},
	// cert-manager.io/v1
	"certificates":   {GVR: gvr("cert-manager.io", "v1", "certificates"), Namespaced: true},
	"issuers":        {GVR: gvr("cert-manager.io", "v1", "issuers"), Namespaced: true},
	"clusterissuers": {GVR: gvr("cert-manager.io", "v1", "clusterissuers")},
}

func gvr(group, version, resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}

// Resolve maps user-facing kind names to watched resources. An unknown name
// is an error listing the known names.
func Resolve(names []string) ([]WatchedResource, error) {
	out := make([]WatchedResource, 0, len(names))
	for _, n := range names {
		key := strings.ToLower(strings.TrimSpace(n))
		w, ok := known[key]
		if !ok {
			return nil, fmt.Errorf("unknown informer %q (known: %s)", n, strings.Join(knownNames(), ", "))
		}
		out = append(out, w)
	}
	return out, nil
}

func knownNames() []string {
	names := make([]string, 0, len(known))
	for n := range known {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
