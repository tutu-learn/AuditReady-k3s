// Package protocol defines the wire types and HTTP client for the AuditReady
// control plane. The protocol is plain HTTP POST with JSON bodies — see
// SERVER.md for the normative specification. Field names are camelCase and
// byte fields are standard-base64 strings, matching the Go JSON tags here.
package protocol

// Inventory event operations.
const (
	OpAdd    = "add"
	OpUpdate = "update"
	OpDelete = "delete"
	OpSync   = "sync" // part of a full resync snapshot
)

// Command verbs.
const (
	VerbApply          = "apply"
	VerbPatch          = "patch"
	VerbDelete         = "delete"
	VerbDrainNode      = "drain-node"
	VerbUncordonNode   = "uncordon-node"
	VerbHelmInstall    = "helm-install"
	VerbHelmUpgrade    = "helm-upgrade"
	VerbHelmUninstall  = "helm-uninstall"
	VerbCertRotate     = "cert-rotate"
	VerbClusterUpgrade = "cluster-upgrade"
)

// Report statuses.
const (
	StatusReceived   = "received"
	StatusValidating = "validating"
	StatusDryRun     = "dry-run"
	StatusApplying   = "applying"
	StatusProgress   = "progress"
	StatusSucceeded  = "succeeded"
	StatusFailed     = "failed"
	StatusRefused    = "refused"
)

// Patch types accepted for VerbPatch.
const (
	PatchStrategic = "strategic"
	PatchMerge     = "merge"
	PatchJSON      = "json"
)

type ResourceRef struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

type InventoryEvent struct {
	Op         string      `json:"op"`
	Ref        ResourceRef `json:"ref"`
	ObjectJSON []byte      `json:"objectJson,omitempty"`
	Timestamp  int64       `json:"timestamp"`
}

// InventoryBatch is the body of POST /audit_ready/k8s/inventory.
type InventoryBatch struct {
	ClusterID string            `json:"clusterId,omitempty"`
	Seq       int64             `json:"seq"`
	Full      bool              `json:"full,omitempty"`
	Events    []*InventoryEvent `json:"events"`
}

// InventoryAck is the response of POST /audit_ready/k8s/inventory.
type InventoryAck struct {
	OK      bool   `json:"ok"`
	LastSeq int64  `json:"lastSeq"`
	Error   string `json:"error,omitempty"`
}

type HelmSpec struct {
	ReleaseName string `json:"releaseName"`
	Namespace   string `json:"namespace"`
	ChartRef    string `json:"chartRef"`
	Version     string `json:"version,omitempty"`
	ValuesYAML  []byte `json:"valuesYaml,omitempty"`
}

type Command struct {
	ID           string      `json:"id"`
	Nonce        string      `json:"nonce"`
	Timestamp    int64       `json:"timestamp"`
	Verb         string      `json:"verb"`
	Target       ResourceRef `json:"target"`
	Payload      []byte      `json:"payload,omitempty"`
	PatchType    string      `json:"patchType,omitempty"`
	Helm         *HelmSpec   `json:"helm,omitempty"`
	ExpectedHash string      `json:"expectedHash,omitempty"`
	Override     bool        `json:"override,omitempty"`
	Signature    string      `json:"signature"`
}

type Report struct {
	ClusterID string `json:"clusterId,omitempty"`
	CommandID string `json:"commandId"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	Diff      string `json:"diff,omitempty"`
	Progress  int32  `json:"progress,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// PollRequest is the body of POST /audit_ready/k8s/poll.
type PollRequest struct {
	ClusterID string    `json:"clusterId,omitempty"`
	Version   string    `json:"version,omitempty"`
	Reports   []*Report `json:"reports,omitempty"`
}

// PollResponse is the response of POST /audit_ready/k8s/poll.
type PollResponse struct {
	OK       bool       `json:"ok"`
	Commands []*Command `json:"commands,omitempty"`
}
