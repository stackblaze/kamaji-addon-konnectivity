# kamaji-addon-konnectivity

Keep the **konnectivity tunnel** healthy on every [Kamaji](https://kamaji.clastix.io/)
tenant cluster — automatically, fleet-wide.

> **What this repo provides**
>
> A small Go controller for the **management cluster** that watches
> `TenantControlPlane` CRs and, for every konnectivity-enabled tenant,
> continuously verifies that the apiserver→in-cluster tunnel actually *serves
> dials* and **self-heals** it when it's up-but-wedged.
>
> It is the outbound-direction companion to
> [stackblaze/kamaji-addon-ingress](https://github.com/stackblaze/kamaji-addon-ingress),
> which handles the inbound direction (pod / external `kubectl` → tenant
> apiserver). The two are complementary, not alternatives.

## Why this exists

In Kamaji the tenant apiserver runs in the **management** cluster and has no
route into the tenant's pod/service network. Two directions have to be solved
separately:

| Direction | Who solves it |
|---|---|
| **into** the apiserver — pods (`kubernetes.default.svc`) + external `kubectl` | `kamaji-addon-ingress` + `clastix/tcp-proxy` (ingress, SNI passthrough, EndpointSlice) |
| **out of** the apiserver — admission webhooks (CNPG/Postgres, MariaDB) + apiserver→kubelet (logs/exec/metrics) | **konnectivity** (this repo keeps it healthy) |

Konnectivity is a reverse tunnel: agents in the tenant dial out to a
konnectivity-server sidecar (`:8132`) in each control-plane pod. Kamaji enables
it but **neither guarantees nor checks that the tunnel actually serves dials**,
and [its docs give no HA/failover guidance and no troubleshooting](https://kamaji.clastix.io/concepts/konnectivity/).

There's a nasty failure mode: the tunnel comes up **registered-but-wedged**. The
agent connects and registers on the konnectivity-server, but every `DIAL_REQ`
times out (~30s) and never completes — so admission webhooks fail with
`context deadline exceeded` and `kubectl logs/exec` hang. We've seen it when an
agent attaches to the *terminating* apiserver pod during a control-plane roll
(2 replicas), and after later disruptions. The tenant's own networking is fine;
only the tunnel data-plane is stuck. A rolling restart of the
`konnectivity-agent` DaemonSet fixes it instantly.

This controller makes that fix automatic and continuous.

## What it does

For each `TenantControlPlane` with `spec.addons.konnectivity` set (opt out per
tenant with `konnectivity.stackblaze.com/verify: "false"`), on a periodic loop:

1. **Verify (dials)** — build a client from the tenant's admin kubeconfig and
   probe **every node's kubelet** through the apiserver proxy
   (`/api/v1/nodes/<node>/proxy/healthz`), several rounds (so requests land on
   both round-robined apiserver replicas), with a short per-call timeout so a
   wedged dial fails fast instead of hanging ~30s. This exercises the exact
   apiserver→in-cluster path that webhooks ride.
2. **Verify (per-server coverage)** — the dial probe is statistical: with an HA
   (multi-replica) control plane it can pass while one replica's
   konnectivity-server has **zero** connected agents (the classic failure —
   agents don't deterministically fan out to every server behind a single VIP).
   So the controller also scrapes **each** konnectivity-server's admin
   `/metrics` (`konnectivity_network_proxy_server_ready_backend_connections`)
   and requires every Running server replica to have ≥1 agent. `covered < total`
   is a coverage gap even when dials happen to pass. Best-effort: if servers
   can't be scraped it falls back to the dial probe.
3. **Self-heal** — if it's reachable but not fully healthy (dials wedged **or**
   a server replica has no agent coverage), roll-restart the tenant's
   `konnectivity-agent` DaemonSet (rate-limited per tenant so a genuinely-broken
   cluster isn't restarted in a loop).
4. **Report** — emit Events on the TCP and export Prometheus metrics, including
   `konnectivity_servers_total` and `konnectivity_servers_covered` so a coverage
   gap is directly alertable (`covered < total`).

5. **Per-agent liveness** — Kamaji's livenessProbe on the agent hits
   `/healthz`, which the agent answers `ok` for as long as the process runs, so a
   disconnected agent — or one that got SIGTERM and sits in "draining" forever
   (`Skipping connectOnce - agent is draining`) — is never restarted. The
   agent's `/readyz` runs the `server-connected` check and fails with
   `no servers connected` when it has zero READY server connections
   (apiserver-network-proxy `pkg/agent/readiness.go`, v0.30–v0.35). Kamaji
   re-asserts the livenessProbe on every reconcile but never touches the
   readinessProbe, so the controller keeps a `/readyz` readinessProbe on the
   DaemonSet and deletes any agent pod that has been NotReady for
   `--agent-restart-after` (90s) — one at a time while other agents are
   connected, all at once when none is. Agents stuck terminating (the node
   can't kill them) are reported, not touched.
6. **Publish the tunnel state for alerting** — on every healthy↔unhealthy
   transition the controller annotates the TCP:
   `konnectivity.stackblaze.com/tunnel-state` (`healthy`|`unhealthy`),
   `…/tunnel-unhealthy-since` (RFC3339) and `…/tunnel-reason`.
   [k8s-healthz](https://github.com/stackblaze/k8s-healthz) (the Kamaji →
   Checkmate bridge) already lists every TCP and turns "unhealthy for more than
   2 minutes" into a 503 on `/tenant/<name>`, i.e. the tenant's existing
   Checkmate monitor goes down and notifies. The controller's own watch ignores
   updates that only touch these keys (or status), so a write never re-triggers
   a probe.

If the apiserver can't be reached at all (still provisioning, transient), it
just re-checks later — it never bounces a cluster that simply isn't ready.

```
                management cluster
   ┌───────────────────────────────────────────┐
   │  kamaji-addon-konnectivity (this)           │
   │     │ watch TenantControlPlane CRs          │
   │     │ read <tcp>-admin-kubeconfig (get-only) │
   │     ▼                                        │
   │  for each konnectivity-enabled tenant:       │
   │     probe apiserver→node/proxy ──┐           │
   │     wedged? → restart agents ────┘           │
   └───────────────────────────────────────────┘
                      │ (probe / restart, via tenant apiserver)
                      ▼
                tenant cluster: konnectivity-agent DaemonSet
```

## Install (Helm)

```bash
helm install kamaji-addon-konnectivity \
  oci://ghcr.io/stackblaze/charts/kamaji-addon-konnectivity \
  --kubeconfig $KUBECONFIG_MGMT \
  --namespace kamaji-system
```

That's it — no per-tenant wiring. Every konnectivity-enabled tenant is verified
and self-healed by default. Opt a tenant out with:

```yaml
metadata:
  annotations:
    konnectivity.stackblaze.com/verify: "false"
```

### Configuration

| Helm value | Flag | Default | Description |
|---|---|---|---|
| `verify.interval` | `--verify-interval` | `3m` | Re-verify a healthy tunnel after this. |
| `verify.recheckInterval` | `--recheck-interval` | `30s` | Re-verify after a bounce / transient failure. |
| `verify.bounceCooldown` | `--bounce-cooldown` | `5m` | Min time between agent bounces per tenant. |
| `verify.probeTimeout` | `--probe-timeout` | `10s` | Per-dial probe timeout (wedged dial otherwise hangs ~30s). |
| `verify.probeRounds` | `--probe-rounds` | `3` | Probe rounds per node (covers both apiserver replicas). |
| `verify.unhealthyThreshold` | `--unhealthy-threshold` | `2` | Consecutive unhealthy verdicts before a DaemonSet bounce. |
| `agents.manage` | `--manage-agents` | `true` | Keep the `/readyz` readinessProbe on the agent DaemonSet + restart disconnected agents. |
| `agents.onlyTenants` | `--manage-agents-tenants` | `[]` | Canary: limit agent management to these TCPs (empty = all). |
| `agents.restartAfter` | `--agent-restart-after` | `90s` | NotReady (disconnected) this long → agent pod deleted. |
| `agents.checkInterval` | `--agent-check-interval` | `30s` | Agent check cadence for a healthy tenant between full probes. |
| `replicaCount` | — | `2` | HA via leader election. |

### Metrics

Exposed on `:8080/metrics`:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `konnectivity_tunnel_healthy` | gauge | `namespace`, `tcp` | 1 if the last probe served dials on every node, else 0. |
| `konnectivity_remediations_total` | counter | `namespace`, `tcp` | Agent-DaemonSet bounces issued. |
| `konnectivity_probe_errors_total` | counter | `namespace`, `tcp` | Probes that couldn't reach the apiserver at all (not a wedge). |
| `konnectivity_servers_total` / `_covered` | gauge | `namespace`, `tcp` | Server replicas vs. replicas with ≥1 agent. |
| `konnectivity_tunnel_unhealthy_seconds` | gauge | `namespace`, `tcp` | Length of the current unhealthy episode (0 when healthy). |
| `konnectivity_agents_desired` / `_ready` | gauge | `namespace`, `tcp` | Agent DaemonSet desired vs. pods connected to ≥1 server. |
| `konnectivity_nodes_without_agent` | gauge | `namespace`, `tcp` | Ready nodes with no running agent (untolerated taint?). |
| `konnectivity_agents_stuck_terminating` | gauge | `namespace`, `tcp` | Agent pods the node cannot kill. |
| `konnectivity_agent_restarts_total` | counter | `namespace`, `tcp` | Disconnected agents the controller restarted. |

The human-facing alert rides k8s-healthz → Checkmate (see 6. above). If you
also scrape these, alert on `konnectivity_tunnel_unhealthy_seconds > 120`, or on a
rising `konnectivity_remediations_total` (a tenant that keeps needing bounces is
a deeper problem — e.g. CP-replica/agent distribution — worth investigating).

## What's in this repo

```
cmd/controller/                       # entrypoint
internal/controller/                  # TenantControlPlane → verify + self-heal reconciler
charts/kamaji-addon-konnectivity/     # Helm chart (management cluster)
Dockerfile.controller                 # multi-arch image build (amd64, arm64)
.github/workflows/build.yml           # CI: vet + build + test + push to ghcr.io
```

## Relationship to provisioning

stackblaze/tenant-clusters verifies konnectivity **once** at provision time
(the `Verify cluster is healthy` gate, before a cluster is marked ready). This
controller is the **standing** counterpart — it catches tunnels that wedge or
regress long after provisioning, with no human in the loop.

## Lessons learned (gotchas)

* **Registered ≠ serving.** A konnectivity agent can connect and register on the
  server while every `DIAL_REQ` still times out. Don't trust "agent Running" —
  probe an actual apiserver→in-cluster dial.
* **Probe every node, several rounds.** With multiple control-plane replicas,
  each apiserver has its own konnectivity-server; a single probe can hit the one
  healthy replica and pass. Repeat to cover them all.
* **Don't bounce on unreachable.** "Can't reach the apiserver" usually means the
  CP isn't ready yet — restarting agents won't help. Only bounce when the
  apiserver is reachable but dials don't complete.
* **Prefer the in-cluster kubeconfig.** The admin secret ships both `admin.conf`
  (public FQDN) and `admin.svc` (in-cluster Service). From the management
  cluster, `admin.svc` avoids a NAT hairpin.

## Cross-links

* **[stackblaze/kamaji-addon-ingress](https://github.com/stackblaze/kamaji-addon-ingress)** — the inbound-to-apiserver companion addon.
* **[Kamaji konnectivity concept](https://kamaji.clastix.io/concepts/konnectivity/)** — upstream docs.

## License

Apache 2.0.
