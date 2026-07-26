// Package validator re-evaluates local safety policy on every command,
// after signature verification. A compromised-but-signed control plane still
// cannot push commands through these gates.
package validator

import (
	"fmt"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/protocol"
)

// Refusal is returned when a command violates local policy. Callers report
// it as StatusRefused rather than StatusFailed.
type Refusal struct{ Reason string }

func (r *Refusal) Error() string { return "refused: " + r.Reason }

// Validator holds the static local policy derived from the operator config.
type Validator struct {
	cfg *config.Config
}

// New returns a Validator bound to cfg.
func New(cfg *config.Config) *Validator { return &Validator{cfg: cfg} }

// Validate checks cmd against local policy. It returns a *Refusal on policy
// violations, nil when the command may proceed.
func (v *Validator) Validate(cmd *protocol.Command) error {
	if cmd == nil {
		return &Refusal{Reason: "nil command"}
	}

	switch cmd.Verb {
	case protocol.VerbApply, protocol.VerbPatch:
		if len(cmd.Payload) == 0 {
			return &Refusal{Reason: fmt.Sprintf("%s requires a non-empty payload", cmd.Verb)}
		}
	case protocol.VerbClusterUpgrade:
		// Safe default: do nothing. Upgrading the cluster from an in-cluster
		// agent is too dangerous for this agent version.
		return &Refusal{Reason: "cluster-upgrade is not supported by this agent version"}
	case protocol.VerbDelete, protocol.VerbDrainNode, protocol.VerbUncordonNode, protocol.VerbCertRotate:
		// Structural checks below.
	case protocol.VerbHelmInstall, protocol.VerbHelmUpgrade, protocol.VerbHelmUninstall:
		if err := v.validateHelm(cmd); err != nil {
			return err
		}
	default:
		return &Refusal{Reason: fmt.Sprintf("unknown verb %q", cmd.Verb)}
	}

	if cmd.Verb == protocol.VerbPatch {
		switch cmd.PatchType {
		case protocol.PatchStrategic, protocol.PatchMerge, protocol.PatchJSON:
		default:
			return &Refusal{Reason: fmt.Sprintf("unsupported patch type %q", cmd.PatchType)}
		}
	}

	if cmd.Verb == protocol.VerbDelete && !v.cfg.AllowDelete {
		return &Refusal{Reason: "delete is disabled by policy (ALLOW_DELETE=false)"}
	}

	// Protected namespaces are off-limits for namespaced targets.
	// Cluster-scoped targets (nodes, namespaces, clusterroles) are exempt.
	if ns := targetNamespace(cmd); ns != "" && v.cfg.ProtectedNamespace(ns) {
		return &Refusal{Reason: fmt.Sprintf("namespace %q is protected", ns)}
	}

	return nil
}

func (v *Validator) validateHelm(cmd *protocol.Command) error {
	h := cmd.Helm
	if h == nil || h.ReleaseName == "" || h.Namespace == "" || h.ChartRef == "" {
		return &Refusal{Reason: "helm command requires a complete helm spec (release name, namespace, chart ref)"}
	}
	return nil
}

// targetNamespace returns the namespace the command operates in: the helm
// namespace for helm verbs, the target namespace otherwise.
func targetNamespace(cmd *protocol.Command) string {
	switch cmd.Verb {
	case protocol.VerbHelmInstall, protocol.VerbHelmUpgrade, protocol.VerbHelmUninstall:
		if cmd.Helm != nil {
			return cmd.Helm.Namespace
		}
		return ""
	}
	return cmd.Target.Namespace
}
