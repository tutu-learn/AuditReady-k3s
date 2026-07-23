# Kubernetes Agent Protocol (HTTP Control Plane)

Normative specification for the in-cluster agent. The control plane is plain
HTTP on the AuditReady server itself — same port as everything else, no
separate listener, no bootstrap handshake, no certificates. If the agent and
this document disagree, the agent is wrong.

Base URL: `https://<server>/audit_ready`. The inventory and poll endpoints
are HTTP POST with JSON bodies; an optional WebSocket endpoint
(`/audit_ready/k8s/ws`) provides real-time command push. Field names are
camelCase; byte fields are base64 (STANDARD) strings; absent optional fields
are omitted.

## Authentication

Every agent request carries a long-lived bearer token:

```
Authorization: Bearer <token>
```

Tokens are scoped to one Kubernetes Cluster doc and issued via the Token
Generator page or the `audit_ready.generate_k8s_agent_token` API method
(Server Admin role). The token determines which cluster the agent acts as;
requests whose payload cluster id disagrees with the token get `403`.
Missing/invalid/revoked tokens get `401`. There is no expiry: a token works
until revoked.

Every authenticated call updates the token's `last_used_at` (and `version`
when the agent reports one); this drives the cluster panel's
connected/offline indicator (connected = call within the last 90 s).

### Token lifecycle

- **Generate**: Token Generator page ("Kubernetes Agent Token" card) or
  `audit_ready.generate_k8s_agent_token { "cluster": "<doc name>" }` →
  `{ "ok": true, "token": "<64 hex>", "cluster": "..." }`. The plaintext is
  shown once; only the argon2 hash is stored. Generating for rotation:
  create the new token, deploy it, then revoke the old one.
- **List**: `audit_ready.list_k8s_agent_tokens {}` →
  `[{ "name" (16-char prefix), "cluster", "enabled", "version",
  "created_at", "last_used_at" }]`.
- **Revoke**: `audit_ready.revoke_k8s_agent { "cluster": "..." }` → disables
  and deletes that cluster's token(s). The agent starts getting `401`
  immediately.

## `POST /audit_ready/k8s/inventory`

Reports cluster state: one full snapshot (`full: true`, op `sync`) on every
(re)connect, then live deltas (`add`/`update`/`delete`). Batches SHOULD be
≤ 200 events.

Request body:

```json
{
  "clusterId": "",
  "seq": 42,
  "full": true,
  "events": [
    {
      "op": "sync",
      "ref": {
        "group": "apps",
        "version": "v1",
        "resource": "deployments",
        "namespace": "default",
        "name": "web"
      },
      "objectJson": "<base64 of raw object JSON>",
      "timestamp": 1700000000
    }
  ]
}
```

`clusterId` is optional; it must match the token's cluster when present.
`seq` is monotonically increasing per agent and survives restarts.

Response (200):

```json
{ "ok": true, "lastSeq": 42 }
```

Semantics:

- The batch is applied atomically (one transaction), last-write-wins per
  object ref; `delete` events remove the row. Unknown ops are skipped with a
  server-side warning and do not fail the batch.
- **Idempotent**: replaying a batch yields the same end state, so an agent
  that did not receive its ack (timeout, connection drop, `5xx`) MUST resend
  the batch with the same `seq`.
- `objectJson` is stored as-is; the agent MUST strip Secret payloads before
  sending (`data`/`stringData` emptied). cert-manager resources appear only
  where those CRDs exist — no special handling on either side.
- Known limitation: full snapshots do not prune objects that disappeared
  while the agent was disconnected; they linger until a `delete` event
  arrives.

Errors: `401` bad token · `403` clusterId mismatch · `500` persistence
failure (resend).

## `POST /audit_ready/k8s/poll`

Polls for queued commands and delivers execution reports. Recommended
interval: 5–30 s.

Request body:

```json
{
  "clusterId": "",
  "version": "0.2.0",
  "reports": [
    {
      "clusterId": "",
      "commandId": "b3f1c2d4-…",
      "status": "succeeded",
      "message": "deployment/web updated",
      "diff": "",
      "progress": 100,
      "timestamp": 1700000100
    }
  ]
}
```

`clusterId` is optional; it must match the token when present. `status` is
one of `received` | `validating` | `dry-run` | `applying` | `progress` |
`succeeded` | `failed` | `refused`.

Response (200):

```json
{
  "ok": true,
  "commands": [
    {
      "id": "b3f1c2d4-…",
      "nonce": "<64 hex>",
      "timestamp": 1700000000,
      "verb": "apply",
      "target": { "group": "apps", "version": "v1",
                  "resource": "deployments", "namespace": "default",
                  "name": "web" },
      "payload": "<base64>",
      "patchType": "strategic",
      "helm": { "releaseName": "…", "namespace": "…", "chartRef": "…",
                "version": "…", "valuesYaml": "<base64>" },
      "expectedHash": "",
      "override": false,
      "signature": "<base64 Ed25519, 64 bytes>"
    }
  ]
}
```

`verb` is one of `apply` | `patch` | `delete` | `drain-node` |
`helm-install` | `helm-upgrade` | `helm-uninstall` | `cert-rotate` |
`cluster-upgrade`. `payload` (manifest/patch bytes) is omitted when empty;
`patchType` (`strategic` | `merge` | `json`) only for `patch`;
`expectedHash` is an optional drift hash.

Semantics:

- Each command is delivered **exactly once** (marked Delivered when handed
  out). There is no redelivery: the agent must persist in-flight commands
  across restarts if it needs them.
- Reports are deduplicated on `(commandId, status, timestamp)` — resending
  the same report is harmless. Reports advance the command's status:
  `succeeded` → Succeeded, `failed` → Failed, `refused` → Refused, anything
  else → Running. Terminal statuses are never overwritten.
- Report conventions: acknowledge with `received` first; validate (verb,
  namespace policy, signature) then run `dry-run` before `applying`; use
  `progress` (with `progress` 0–100) for long operations; end with exactly
  one of `succeeded` / `failed` / `refused`. Refuse (with `message`
  explaining why) anything that fails verification or policy.

Errors: `401` bad token · `403` clusterId mismatch · `500` (resend later).

## `GET /audit_ready/k8s/ws` (real-time command channel)

Optional fast path modeled on the host-agent tunnel. The agent opens a
WebSocket here and keeps it open; the server pushes signed commands the
moment they are queued instead of waiting for the next poll. HTTP polling
remains the fallback: agents with a dead socket keep calling `/k8s/poll`, so
the server must tolerate either delivery path. Frames are JSON text frames
with a snake_case `type` tag.

### Handshake

The upgrade itself carries no credentials. The agent's first frame MUST be:

```json
{ "type": "agent_hello", "token": "<64 hex>", "clusterId": "…", "version": "0.2.0" }
```

Validate the token exactly as for the HTTP endpoints (401 semantics) and the
clusterId when present (403 semantics). Reply:

```json
{ "type": "hello_ack", "accepted": true }
```

On failure send `{ "type": "hello_ack", "accepted": false, "message": "unauthorized" }`
and close. Agents reconnect with backoff (1 s → 30 s, ±20 % jitter); apply
the same per-agent reconnect rate limiting as the tunnel broker.

### Server → agent frames

```json
{ "type": "command", "command": { … } }
```

`command` has the identical shape, signing, and canonical payload as commands
returned by `/k8s/poll` — the agent runs the exact same verification pipeline
(signature, timestamp, nonce, policy) regardless of delivery path.

```json
{ "type": "error", "message": "…" }
```

Informational or fatal; the agent logs it. Close the socket after fatal
errors.

### Agent → server frames

```json
{ "type": "report", "report": { … } }
```

`report` has the identical shape and semantics as reports in the poll request
body — dedupe on `(commandId, status, timestamp)`, advance command status,
terminal statuses never overwritten. When the socket is down the agent queues
reports and flushes them via the next `/k8s/poll`, so the server must accept
the same report over either channel.

### Delivery semantics

- Sending a `command` frame marks the command Delivered, exactly as handing
  it out in a poll response does. **A command pushed over WS must not later
  be returned by `/k8s/poll`** — but failover races (socket dies mid-push)
  may still cause double delivery; the agent dedupes silently by command id,
  so duplicates are harmless.
- If the socket for a cluster is not connected, leave its commands Queued for
  the poll path.
- Keepalive: server sends protocol-level Ping every 20 s and drops
  connections idle for 60 s (same as the tunnel broker); the agent pings
  every 25 s and treats ~60 s of silence as dead.
- Max frame size 256 KiB. One connection per cluster: a new `agent_hello` for
  a cluster evicts the previous socket.

## Command signature verification (REQUIRED)

The server signs every command with its Ed25519 key. Fetch the public key
once (Server Admin) and pin it in the agent's config:

```
audit_ready.k8s_server_public_key {} → { "ok": true, "public_key": "<base64, 32 bytes>" }
```

Canonical signing payload — newline-separated (`\n`) UTF-8 lines, exactly
this order, no trailing newline:

```
nonce
timestamp            (decimal int64)
id
verb
target.group
target.version
target.resource
target.namespace
target.name
patchType            (empty string when unset)
sha256hex(payload)   (lowercase hex sha256 of the RAW, base64-decoded bytes;
                      sha256 of empty input when unset)
expectedHash         (empty string when unset)
override             ("true" or "false")
```

When `helm` is present, append 5 lines:

```
helm.releaseName
helm.namespace
helm.chartRef
helm.version
sha256hex(helm.valuesYaml)
```

Verify the Ed25519 signature (base64-decoded from `signature`) over those
bytes. Refuse any command whose signature does not verify, whose nonce has
been seen before, or whose timestamp is implausibly old.

## Observing state (Desk/portal, not for agents)

- `audit_ready.k8s_inventory {cluster, resource?, namespace?}` — stored
  objects (decoded JSON), newest first, max 500.
- `audit_ready.k8s_commands {cluster?, status?, limit?}` — command lifecycle
  with the latest report per command.
- `audit_ready.cluster_detail {name}` — includes `agent: { connected,
  version, last_seen, inventory_counts }` or `null`.

## Smoke-test without a real cluster

`examples/k8s_agent_sim.rs` is a runnable simulated agent:

```
cargo run -p audit_ready --example k8s_agent_sim -- \
  --addr http://127.0.0.1:8000 --token <token> [--pubkey <base64>] [--follow]
```

It POSTs one full inventory snapshot (Deployment, Service, stripped Secret),
then polls: prints received commands (verifying signatures when `--pubkey`
is given) and reports `received → dry-run → applying → succeeded` on the next
poll, then exits (`--follow` keeps polling every 5 s). The `seq` counter
persists in `--state-dir` (default `/tmp/k8s-agent-sim`).

## Where the state lives

All tables are in the site SQLite database and created idempotently at
startup: `k8s_agent_token` (tokens; argon2 hashes only), `k8s_server_keys`
(Ed25519 signing key), `k8s_inventory`, `k8s_command`, `k8s_command_report`.
Timestamps are INTEGER epoch seconds. (`k8s_ca`, `k8s_bootstrap_token`,
`k8s_agent` from the retired gRPC design are no longer created; stale copies
on older sites are harmless.)

---

## Agent-side implementation notes (this repo)

- HTTP client, wire types, and signing live in `internal/protocol/`
  (`http.go`, `types.go`, `sign.go`, `ws.go`); the WebSocket fast path lives
  in `internal/wsclient/` and feeds the same verification pipeline as polled
  commands (set `WS_ENABLED=false` to run poll-only).
- The uploader persists `seq` in `$SPILL_DIR/seq` and resends unacked batches
  with the same `seq`; a full snapshot is sent on startup and after every
  failed send.
- The poll loop sleeps `POLL_INTERVAL` (default 30s) between successful
  polls and backs off 1s → 30s (±20% jitter) on errors; reports are queued
  (cap 1000, oldest dropped) and resent until a poll succeeds.
- 401/403 surface as `protocol.AuthError` and are logged loudly ("check
  SERVER_TOKEN"); the agent keeps backing off — rotate the token and restart
  the pod to recover.
- A local mock of this protocol ships in `cmd/mockserver`
  (`go run ./cmd/mockserver -token dev-token -command-file commands.json` —
  it prints `SERVER_TOKEN` and `SERVER_PUBLIC_KEY` for the agent's env).
