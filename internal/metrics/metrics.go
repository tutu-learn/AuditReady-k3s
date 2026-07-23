// Package metrics exposes the operator's Prometheus metrics on a private
// registry served by internal/server. All subsystems report through the
// package-level helpers below.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Registry holds all operator metrics; served on /metrics.
var Registry = prometheus.NewRegistry()

var factory = promauto.With(Registry)

var (
	cacheSyncLag = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "k8s_agent_cache_sync_lag_seconds",
		Help: "Seconds since the informer cache for a resource last processed an event.",
	}, []string{"resource"})

	commandsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_agent_commands_total",
		Help: "Commands processed, by verb and terminal result.",
	}, []string{"verb", "result"})

	tunnelConnected = factory.NewGauge(prometheus.GaugeOpts{
		Name: "k8s_agent_tunnel_connected",
		Help: "1 while the mTLS tunnel to the control plane is established.",
	})

	drainProgress = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "k8s_agent_drain_progress_percent",
		Help: "Drain progress for a node, 0-100.",
	}, []string{"node"})

	uploadQueueDepth = factory.NewGauge(prometheus.GaugeOpts{
		Name: "k8s_agent_upload_queue_depth",
		Help: "Inventory events currently buffered for upload.",
	})

	inventoryEventsSent = factory.NewCounter(prometheus.CounterOpts{
		Name: "k8s_agent_inventory_events_sent_total",
		Help: "Inventory events successfully streamed to the control plane.",
	})
)

// ObserveCacheSyncLag records the sync lag for a watched resource.
func ObserveCacheSyncLag(resource string, seconds float64) {
	cacheSyncLag.WithLabelValues(resource).Set(seconds)
}

// IncCommand counts a command reaching a terminal result
// (succeeded, failed, refused).
func IncCommand(verb, result string) {
	commandsTotal.WithLabelValues(verb, result).Inc()
}

// SetTunnelConnected records tunnel connectivity.
func SetTunnelConnected(connected bool) {
	if connected {
		tunnelConnected.Set(1)
	} else {
		tunnelConnected.Set(0)
	}
}

// SetDrainProgress records drain progress for a node.
func SetDrainProgress(node string, pct float64) {
	drainProgress.WithLabelValues(node).Set(pct)
}

// ClearDrainProgress removes the drain progress series for a node.
func ClearDrainProgress(node string) {
	drainProgress.DeleteLabelValues(node)
}

// SetQueueDepth records the upload buffer depth.
func SetQueueDepth(n int) {
	uploadQueueDepth.Set(float64(n))
}

// IncInventoryEvents counts events streamed to the control plane.
func IncInventoryEvents(n int) {
	inventoryEventsSent.Add(float64(n))
}
