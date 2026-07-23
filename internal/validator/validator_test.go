package validator

import (
	"errors"
	"strings"
	"testing"
	"time"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/protocol"
)

func testConfig() *config.Config {
	return &config.Config{
		ClusterID:           "test",
		WriterEnabled:       true,
		DryRunFirst:         true,
		DriftPolicy:         config.DriftRefuse,
		AllowDelete:         true,
		ProtectedNamespaces: []string{"kube-system", "k8s-agent-system"},
		DrainStallTimeout:   time.Minute,
	}
}

func applyCmd() *protocol.Command {
	return &protocol.Command{
		ID:      "cmd-1",
		Verb:    protocol.VerbApply,
		Target:  protocol.ResourceRef{Version: "v1", Resource: "configmaps", Namespace: "default", Name: "cm"},
		Payload: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: default\n"),
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*config.Config)
		cmd  func(*protocol.Command)
		// want, when non-empty, is a substring of the refusal reason.
		want string
	}{
		{name: "valid apply"},
		{
			name: "unknown verb",
			cmd:  func(c *protocol.Command) { c.Verb = "rm-rf" },
			want: `unknown verb "rm-rf"`,
		},
		{
			name: "cluster-upgrade refused",
			cmd:  func(c *protocol.Command) { c.Verb = protocol.VerbClusterUpgrade; c.Payload = nil },
			want: "cluster-upgrade is not supported by this agent version",
		},
		{
			name: "apply without payload",
			cmd:  func(c *protocol.Command) { c.Payload = nil },
			want: "requires a non-empty payload",
		},
		{
			name: "patch without payload",
			cmd: func(c *protocol.Command) {
				c.Verb = protocol.VerbPatch
				c.PatchType = protocol.PatchMerge
				c.Payload = nil
			},
			want: "requires a non-empty payload",
		},
		{
			name: "patch with bad type",
			cmd:  func(c *protocol.Command) { c.Verb = protocol.VerbPatch; c.PatchType = "sql" },
			want: `unsupported patch type "sql"`,
		},
		{
			name: "patch strategic ok",
			cmd:  func(c *protocol.Command) { c.Verb = protocol.VerbPatch; c.PatchType = protocol.PatchStrategic },
		},
		{
			name: "patch json ok",
			cmd:  func(c *protocol.Command) { c.Verb = protocol.VerbPatch; c.PatchType = protocol.PatchJSON },
		},
		{
			name: "protected namespace",
			cmd:  func(c *protocol.Command) { c.Target.Namespace = "kube-system" },
			want: `namespace "kube-system" is protected`,
		},
		{
			name: "cluster-scoped target exempt",
			cmd: func(c *protocol.Command) {
				c.Target = protocol.ResourceRef{Version: "v1", Resource: "namespaces", Name: "kube-system"}
			},
		},
		{
			name: "delete allowed by default",
			cmd:  func(c *protocol.Command) { c.Verb = protocol.VerbDelete; c.Payload = nil },
		},
		{
			name: "delete disabled",
			cfg:  func(cfg *config.Config) { cfg.AllowDelete = false },
			cmd:  func(c *protocol.Command) { c.Verb = protocol.VerbDelete; c.Payload = nil },
			want: "delete is disabled by policy",
		},
		{
			name: "helm install ok",
			cmd: func(c *protocol.Command) {
				c.Verb = protocol.VerbHelmInstall
				c.Payload = nil
				c.Helm = &protocol.HelmSpec{ReleaseName: "app", Namespace: "apps", ChartRef: "bitnami/nginx"}
			},
		},
		{
			name: "helm missing spec",
			cmd:  func(c *protocol.Command) { c.Verb = protocol.VerbHelmInstall; c.Payload = nil },
			want: "requires a complete helm spec",
		},
		{
			name: "helm missing release name",
			cmd: func(c *protocol.Command) {
				c.Verb = protocol.VerbHelmUpgrade
				c.Payload = nil
				c.Helm = &protocol.HelmSpec{Namespace: "apps", ChartRef: "bitnami/nginx"}
			},
			want: "requires a complete helm spec",
		},
		{
			name: "helm missing chart ref",
			cmd: func(c *protocol.Command) {
				c.Verb = protocol.VerbHelmUninstall
				c.Payload = nil
				c.Helm = &protocol.HelmSpec{ReleaseName: "app", Namespace: "apps"}
			},
			want: "requires a complete helm spec",
		},
		{
			name: "helm protected namespace",
			cmd: func(c *protocol.Command) {
				c.Verb = protocol.VerbHelmInstall
				c.Payload = nil
				c.Helm = &protocol.HelmSpec{ReleaseName: "app", Namespace: "kube-system", ChartRef: "bitnami/nginx"}
			},
			want: `namespace "kube-system" is protected`,
		},
		{
			name: "drain node cluster-scoped",
			cmd: func(c *protocol.Command) {
				c.Verb = protocol.VerbDrainNode
				c.Payload = nil
				c.Target = protocol.ResourceRef{Version: "v1", Resource: "nodes", Name: "node-1"}
			},
		},
		{
			name: "cert-rotate protected namespace",
			cmd: func(c *protocol.Command) {
				c.Verb = protocol.VerbCertRotate
				c.Payload = nil
				c.Target = protocol.ResourceRef{
					Group: "cert-manager.io", Version: "v1", Resource: "certificates",
					Namespace: "kube-system", Name: "tls",
				}
			},
			want: `namespace "kube-system" is protected`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			if tt.cfg != nil {
				tt.cfg(cfg)
			}
			cmd := applyCmd()
			if tt.cmd != nil {
				tt.cmd(cmd)
			}

			err := New(cfg).Validate(cmd)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("expected no refusal, got %v", err)
				}
				return
			}
			var ref *Refusal
			if !errors.As(err, &ref) {
				t.Fatalf("expected *Refusal, got %T: %v", err, err)
			}
			if !strings.Contains(ref.Reason, tt.want) {
				t.Fatalf("reason %q does not contain %q", ref.Reason, tt.want)
			}
		})
	}
}

func TestRefusalError(t *testing.T) {
	r := &Refusal{Reason: "boom"}
	if r.Error() != "refused: boom" {
		t.Fatalf("unexpected Error(): %q", r.Error())
	}
}
