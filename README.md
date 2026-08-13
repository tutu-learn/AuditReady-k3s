# k8s-agent-operator

In-cluster agent for the Kubernetes management tool. One deployment per cluster. Reads cluster state and executes signed commands received from the control plane server.

## What it does

- Watches every Kubernetes resource of interest via shared informers and streams changes to the server
- Receives signed commands over a persistent WebSocket (real-time push, `WS_ENABLED` default on) with HTTP polling every `POLL_INTERVAL` (default 30s) as the fallback, and executes them: install/upgrade/uninstall Helm charts, patch resources, rotate certificates, drain and uncordon nodes (`cluster-upgrade` is currently refused as unsupported — the agent errs toward doing nothing)
- Re-evaluates policy locally before applying — a compromised server cannot push forbidden changes
- Dry-runs every mutation before applying
- Detects drift between last-known state and live state, refuses conflicting writes without an override
- Reports progress and results back to the server
- Never exposes an inbound port — all traffic is outbound HTTPS only, authenticated with a bearer token

## Architecture

The operator is one binary but runs as two logically separate subsystems, each with its own ServiceAccount:

```
                 [ control plane server ]
                          │
                   HTTPS POST (outbound only)
                          │
   ┌──────────────────────┴──────────────────────┐
   │              operator (1 replica)           │
   ├─────────────────────┬───────────────────────┤
   │  Information        │  Control              │
   │  gatherer           │  executor             │
   │  (read-only)        │  (read-write)         │
   │                     │                       │
   │  - Shared informers │  - Command poller     │
   │  - Local cache      │  - Signature verify   │
   │  - Diff uploader    │  - Policy re-eval     │
   │                     │  - Dry-run            │
   │                     │  - Safety checks      │
   │                     │  - Executor           │
   ├─────────────────────┼───────────────────────┤
   │ SA: agent-reader    │ SA: agent-writer      │
   │ (get, list, watch)  │ (scoped write verbs)  │
   └──────────┬──────────┴──────────┬────────────┘
              │                     │
              └──────────┬──────────┘
                         │
                  [ kube-apiserver ]
                  RBAC decides per call
```

Two ServiceAccounts, two token files (`/var/run/secrets/agent-reader`, `/var/run/secrets/agent-writer`), two ClusterRoleBindings. The reader token is a projected serviceAccountToken volume; the writer token comes from a classic token Secret because projections can only mint tokens for the pod's own SA. The read path holds only a read-only dynamic client and physically cannot mutate the cluster. The writer is bound cluster-wide to the built-in `admin` ClusterRole (all namespaced resources, every namespace) plus namespace read; neither account gets `cluster-admin`. If the writer token is absent the process forces read-only mode.

## Requirements

- Kubernetes 1.27+
- Helm 3.12+ (for install)
- Egress to your control plane endpoint on port 443
- cert-manager installed in the cluster (only if you want certificate management features)

## Installation

Get an agent token from the control plane web UI (Clusters → Add cluster). Then:

```bash
helm install k8s-agent oci://ghcr.io/tutu-learn/charts/k8s-agent \
  --namespace k8s-agent-system \
  --create-namespace \
  --set server.endpoint=https://control-plane.your-org.example/audit_ready \
  --set server.publicKey=<ed25519-public-key-from-ui> \
  --set server.token=<agent-token-from-ui>
```

The token is long-lived and scoped to one cluster — it works until revoked. To rotate it: generate a new token in the control plane UI, deploy it to the agent, then revoke the old one.

### Images and chart artifacts (GHCR)

`.github/workflows/ci.yaml` runs tests on every push/PR, and on pushes to
`main` and `v*` tags publishes:

- **Image** — `ghcr.io/tutu-learn/k8s-agent-operator` (linux/amd64+arm64)
- **Chart** — `oci://ghcr.io/tutu-learn/charts/k8s-agent`

Versioning is automatic: every green `main` build publishes chart and image
as `0.1.<CI run number>` (plus `latest` for the image) — a stable,
monotonically increasing version, so `helm install oci://...` with no
`--version` always gets the newest build, and `helm upgrade` picks up each
new push. Tagging `v1.2.3` additionally publishes chart and image as `1.2.3`
for explicit releases.

First-time push note:
GHCR packages inherit repo visibility — after the first CI run, make the
`k8s-agent-operator` and `charts/k8s-agent` packages public (or connect them
to the repo) under github.com/users/tutu-learn/packages. If they stay
private, create an image pull secret:

```bash
kubectl -n k8s-agent-system create secret docker-registry ghcr \
  --docker-server=ghcr.io --docker-username=<user> --docker-password=<PAT>
```

Verify:

```bash
kubectl -n k8s-agent-system get pods
kubectl -n k8s-agent-system logs -l app=k8s-agent
```

The cluster appears in the control plane UI within a minute of first sync.

## Upgrade to latest

```bash
helm upgrade k8s-agent oci://ghcr.io/tutu-learn/charts/k8s-agent -n k8s-agent-system --reuse-values
```

Omitting `--version` pulls the newest published chart build, which also pins
the matching new image via `appVersion`. Watch the rollout with:

```bash
kubectl rollout status deployment/k8s-agent -n k8s-agent-system
```

## Uninstall

```bash
helm uninstall k8s-agent -n k8s-agent-system
kubectl delete namespace k8s-agent-system
```

The operator is a management plane, not a data plane. Removing it does not affect running workloads.

## What the operator watches

Configurable in `values.yaml` under `informers.enabled`. Defaults:

- Core: `Deployment`, `StatefulSet`, `DaemonSet`, `ReplicaSet`, `Pod`, `Service`, `Ingress`, `ConfigMap`, `Secret`, `Node`, `Namespace`, `PersistentVolumeClaim`
- RBAC: `Role`, `RoleBinding`, `ClusterRole`, `ClusterRoleBinding`, `ServiceAccount`
- Networking: `NetworkPolicy`
- Workload safety: `PodDisruptionBudget`
- Helm: `Secret` with label `owner=helm`
- cert-manager: `Certificate`, `Issuer`, `ClusterIssuer` (skipped gracefully if the CRDs are absent)

Secret *values* are never uploaded. Only metadata, labels, annotations, type, and the set of data *keys* (values blanked).

## ServiceAccount permissions

The Helm chart creates two ServiceAccounts.

**`agent-reader`** — bound to a ClusterRole granting `get`, `list`, `watch` on all watched kinds. Read only.

**`agent-writer`** — bound cluster-wide to the built-in `admin` ClusterRole, granting full `create`, `update`, `patch`, `delete` (and read) on all namespaced resources in every namespace, including Roles/RoleBindings inside namespaces. A second ClusterRoleBinding adds `get`, `list`, `watch` on Namespaces, which the operator needs to validate deploy targets.

**Not granted:** cluster-scoped writes — the writer cannot create or modify CRDs, ClusterRoles/ClusterRoleBindings, Nodes, or Namespaces themselves, and gets no `impersonate` / `escalate` / `bind` verbs on cluster-level RBAC. Treat the writer token as a high-value credential regardless: cluster-wide `admin` can read every Secret in the cluster.

Review the exact grants before installing: `helm template ... | grep -A5 rules`.

## Configuration

Full values reference in `charts/k8s-agent/values.yaml`. The binary reads every setting from an environment variable of the same name (`SERVER_ENDPOINT`, `SERVER_TOKEN`, `SERVER_PUBLIC_KEY`, `CLUSTER_ID`, `POLL_INTERVAL`, `WS_ENABLED`, `READER_TOKEN_PATH`, `WRITER_TOKEN_PATH`, `SPILL_DIR`, `WRITER_ENABLED`, `DRY_RUN_FIRST`, `DRIFT_POLICY`, `ALLOW_DELETE`, `INFORMERS_ENABLED`, `PROTECTED_NAMESPACES`, `METRICS_ADDR`, `LOG_LEVEL`, `DRAIN_STALL_TIMEOUT`). Common settings:

```yaml
server:
  endpoint: https://control-plane.your-org.example/audit_ready
  publicKey: <ed25519 public key>
  token: <agent token from the control plane UI>

informers:
  enabled:
    - deployments
    - services
    # ...

writer:
  enabled: true              # set false for read-only mode
  dryRunFirst: true          # always dry-run before apply
  driftPolicy: refuse        # refuse | overwrite | overwrite-with-approval

resources:
  requests: { cpu: 100m, memory: 200Mi }
  limits:   { cpu: 500m, memory: 500Mi }

networkPolicy:
  enabled: true              # only allow egress to server.endpoint

logging:
  level: info                # debug, info, warn, error
  redactSecrets: true        # never log secret values (always on regardless)

poller:
  interval: 30s              # how often the operator polls for commands
```

## Read-only mode

For evaluation or high-caution environments:

```bash
helm install ... --set writer.enabled=false
```

The `agent-writer` ServiceAccount is not created. The operator refuses any inbound command. Inventory upload works as normal.

## Failure behaviour

| Scenario | Behaviour |
|---|---|
| HTTPS calls to server fail | Keep watching locally, buffer deltas (10k in-memory, disk spill), polls fail and back off (commands are never pushed — nothing arrives while disconnected), retry with backoff; failed batches are resent with the same sequence number |
| API server unreachable | Both subsystems back off with jitter; report as cluster health warning |
| Local policy re-eval fails | Refuse the command even if server signed it |
| Drain stalls past 15 min | Pause operation, notify server, wait for human |
| Detected drift on target resource | Refuse the update; surface diff; require override approval |
| Operator pod killed | Cluster keeps running exactly as it was; informers rebuild from scratch on restart |

The operator errs toward doing nothing rather than doing the wrong thing.

## Observability

- Prometheus metrics on `:9090/metrics` — cache sync lag, command counts by verb and result, server connectivity, drain progress, upload queue depth
- Structured JSON logs to stdout — one line per event
- Health endpoints: `/healthz` (liveness), `/readyz` (readiness; fails while caches are syncing)

## Development

```bash
git clone <repo>
cd k8s-agent-operator
make deps
make test
make run.local KUBECONFIG=~/.kube/config \
  SERVER_ENDPOINT=http://127.0.0.1:8443/audit_ready \
  SERVER_TOKEN=dev-token \
  SERVER_PUBLIC_KEY=<key printed by mockserver>
```

Local end-to-end without a cluster: run the mock control plane (`go run ./cmd/mockserver -addr 127.0.0.1:8443 -token dev-token -command-file commands.json` — it prints the `SERVER_TOKEN` and `SERVER_PUBLIC_KEY` to set), then:

```bash
make run.local KUBECONFIG=~/.kube/config \
  SERVER_ENDPOINT=http://127.0.0.1:8443/audit_ready \
  SERVER_TOKEN=dev-token \
  SERVER_PUBLIC_KEY=<key printed by mockserver>
```

For full end-to-end testing (requires kind and helm):

```bash
make e2e   # spins up kind, installs operator, drives commands via mock server
```

## Project layout

```
cmd/
  operator/         # main entrypoint
  mockserver/       # mock control plane for local dev and tests
internal/
  config/           # env-based configuration
  informers/        # shared informer setup, cache, secret stripping
  uploader/         # delta buffering (10k + disk spill) and inventory upload
  protocol/         # wire types, Ed25519 signing, HTTP client, WS frames
  wsclient/         # WebSocket command channel (real-time push)
  receiver/         # command polling and signature/nonce/timestamp verify
  validator/        # local policy re-evaluation
  executor/         # apply, patch, delete, drain, helm ops, cert rotation
  drift/            # drift detection (content hashes)
  metrics/          # Prometheus
  server/           # /metrics, /healthz, /readyz listener
charts/
  k8s-agent/        # Helm chart for install
```

## Security notes

- Secret values are never logged, never uploaded to the server, never included in dry-run output or drift diffs
- The bearer token (`SERVER_TOKEN`) is the only credential the agent holds — it is long-lived, scoped to one cluster, and works until revoked; store it as a Secret and rotate it via the control plane UI (issue new, deploy, revoke old)
- Command signatures use Ed25519 over a canonical payload; nonce (10k replay cache) and a ±5 minute timestamp window prevent replay
- Every write is dry-run against the API server first (when `dryRunFirst`), and reported before and after it hits the API server; when the server is unreachable, up to 10k events are buffered with disk spill
- Drift is tracked with a content hash in the `k8s-agent.io/last-applied-hash` annotation on managed resources
- Report vulnerabilities to `security@your-org.example`

## License

TBD.
