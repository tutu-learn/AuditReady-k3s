// Package config loads the operator's runtime configuration from environment
// variables. Every setting has an env var; defaults match the Helm chart.
package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Drift policies.
const (
	DriftRefuse                = "refuse"
	DriftOverwrite             = "overwrite"
	DriftOverwriteWithApproval = "overwrite-with-approval"
)

type Config struct {
	// Control plane connection (plain HTTP, bearer-token auth — see SERVER.md).
	ServerEndpoint  string // base URL, e.g. https://server/audit_ready
	ServerToken     string // long-lived bearer token, scoped to one cluster
	ServerPublicKey string // base64 Ed25519 public key used to verify commands
	ClusterID       string // optional; must match the token's cluster when set
	PollInterval    time.Duration
	WSEnabled       bool // real-time command channel at <endpoint>/k8s/ws

	// Paths.
	ReaderTokenPath string
	WriterTokenPath string
	SpillDir        string

	// Writer behaviour.
	WriterEnabled bool
	DryRunFirst   bool
	DriftPolicy   string
	AllowDelete   bool

	// Informers.
	InformersEnabled []string

	// Local policy re-evaluation.
	ProtectedNamespaces []string

	// Observability.
	MetricsAddr string
	LogLevel    string

	// Drain safety.
	DrainStallTimeout time.Duration
}

// Load reads configuration from the environment. It returns an error when a
// required value (server endpoint, public key) is missing or malformed.
func Load() (*Config, error) {
	c := &Config{
		ServerEndpoint:      os.Getenv("SERVER_ENDPOINT"),
		ServerToken:         os.Getenv("SERVER_TOKEN"),
		ServerPublicKey:     os.Getenv("SERVER_PUBLIC_KEY"),
		ClusterID:           os.Getenv("CLUSTER_ID"),
		PollInterval:        envDuration("POLL_INTERVAL", 30*time.Second),
		WSEnabled:           envBool("WS_ENABLED", true),
		ReaderTokenPath:     envOr("READER_TOKEN_PATH", "/var/run/secrets/agent-reader/token"),
		WriterTokenPath:     envOr("WRITER_TOKEN_PATH", "/var/run/secrets/agent-writer/token"),
		SpillDir:            envOr("SPILL_DIR", "/var/lib/k8s-agent/spill"),
		WriterEnabled:       envBool("WRITER_ENABLED", true),
		DryRunFirst:         envBool("DRY_RUN_FIRST", true),
		DriftPolicy:         envOr("DRIFT_POLICY", DriftRefuse),
		AllowDelete:         envBool("ALLOW_DELETE", true),
		InformersEnabled:    envList("INFORMERS_ENABLED", DefaultInformers),
		ProtectedNamespaces: envList("PROTECTED_NAMESPACES", []string{"kube-system", "k8s-agent-system"}),
		MetricsAddr:         envOr("METRICS_ADDR", ":9090"),
		LogLevel:            envOr("LOG_LEVEL", "info"),
		DrainStallTimeout:   envDuration("DRAIN_STALL_TIMEOUT", 15*time.Minute),
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate checks required fields and value domains.
func (c *Config) Validate() error {
	if c.ServerEndpoint == "" {
		return fmt.Errorf("SERVER_ENDPOINT is required")
	}
	if c.ServerToken == "" {
		return fmt.Errorf("SERVER_TOKEN is required")
	}
	if _, err := c.PublicKey(); err != nil {
		return fmt.Errorf("SERVER_PUBLIC_KEY: %w", err)
	}
	switch c.DriftPolicy {
	case DriftRefuse, DriftOverwrite, DriftOverwriteWithApproval:
	default:
		return fmt.Errorf("DRIFT_POLICY %q must be one of %s, %s, %s",
			c.DriftPolicy, DriftRefuse, DriftOverwrite, DriftOverwriteWithApproval)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("LOG_LEVEL %q must be debug, info, warn or error", c.LogLevel)
	}
	return nil
}

// PublicKey decodes the configured base64 Ed25519 public key.
func (c *Config) PublicKey() (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(c.ServerPublicKey)
	if err != nil {
		return nil, fmt.Errorf("not valid base64: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("expected %d bytes, got %d", ed25519.PublicKeySize, len(raw))
	}
	return ed25519.PublicKey(raw), nil
}

// EndpointBase returns the endpoint with any trailing slash trimmed, ready
// for path joining.
func (c *Config) EndpointBase() string {
	return strings.TrimRight(c.ServerEndpoint, "/")
}

// ProtectedNamespace reports whether ns is protected from mutation.
func (c *Config) ProtectedNamespace(ns string) bool {
	for _, p := range c.ProtectedNamespaces {
		if p == ns {
			return true
		}
	}
	return false
}

// DefaultInformers is the watch set used when INFORMERS_ENABLED is unset.
var DefaultInformers = []string{
	"deployments", "statefulsets", "daemonsets", "replicasets",
	"pods", "services", "ingresses", "configmaps", "secrets", "nodes",
	"namespaces", "persistentvolumeclaims",
	"roles", "rolebindings", "clusterroles", "clusterrolebindings",
	"serviceaccounts", "networkpolicies", "poddisruptionbudgets",
	"certificates", "issuers", "clusterissuers",
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envList(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
