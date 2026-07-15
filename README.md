# lib-service-discovery

Service discovery library (`lib-service-discovery`) backed by HashiCorp Consul, following Lerian's `lib-commons` conventions.

## Requirements

- Go `1.26` or newer
- HashiCorp Consul `1.19` or newer (only when `SERVICE_DISCOVERY_ENABLED=true`)

## Installation

```bash
go get github.com/LerianStudio/lib-service-discovery
```

## Breaking changes (v1.0.0)

`v1.0.0` is the first stable release. It carries the dual-endpoint / managed
watch-and-cache redesign and is **not source-compatible** with `v0.6.x`:

- `Service.EndpointFor(view)` now returns `(Endpoint, error)` — an `External`
  view against an internal-only service returns `ErrEndpointViewUnavailable`.
- `Config.Validate` no longer requires an advertise address when discovery is
  enabled (consumer-only Managers are valid); the "at least one endpoint"
  requirement (`ErrNoEndpoint`) moved to `Register`.
- `Config.AllowStale` changed from `bool` to `*bool` (nil defaults to `true`).
- Removed the never-released `WithResolveCache` option and `Config.ResolveTimeout`
  knob — one-shot `Resolve`/`ResolveEndpoint` are now backed by a managed
  watch-and-cache resolver (Consul stays off the request path).
- `Service.Address`/`Port`/`Scheme` are **deprecated** (root-routable mirror);
  use `Service.External` / `Service.Internal`.

## What is in this library

### `lib-service-discovery`

A service discovery abstraction. Its fallback behaviour has three modes:

| Mode | Behaviour |
|---|---|
| Disabled | All operations are no-ops. `Resolve` returns the fallback address directly. |
| Enabled with fallback | Serves the discovered instance; on a cold cache falls back to a static address. |
| Enabled without fallback | Serves the discovered instance; returns an error when none is cached. |

**Resolution model — watch-and-cache (single pattern):**

There is one resolution pattern, not a query-per-request one. The first resolve of
a name lazily seeds a per-name resolver (a single bounded Consul read,
`SD_SEED_TIMEOUT`) and starts **one** background watch for that name. Every
subsequent `Resolve` / `ResolveService` / `ResolveEndpoint` /
`ResolvePreferredEndpoint` reads the **cached** `Service` and **never contacts
Consul on the request path**, so a slow, dead, or partitioned discovery server can
never park a caller. The watch keeps the cache fresh (last-known-good on a
transient failure); the seed is fail-open (serves the fallback until the watch
converges). One watcher exists per distinct name for the Manager's lifetime and is
stopped by `Manager.Close()`.

> **No client-side load balancing.** The cache holds **one** instance per name, so
> consecutive resolves return the same address until the catalog changes. This
> library does not balance across replicas per request — spreading traffic across a
> service's pods is the downstream's job: resolve to the **Kubernetes Service DNS
> name** (which distributes across ready pods; an ingress handles the external
> path), not to an individual pod.

**Key types:**

- `Manager` — entry point; created with `New(cfg, opts...)`.
- `Registry` — interface for the Consul backend; can be replaced by an in-memory stub in tests.
- `Service` / `HealthCheck` / `Event` — domain types.

**Functional options:**

```go
libsd.WithLogger(logger)   // inject a lib-commons log.Logger
```

## Usage

```go
cfg := libsd.ConfigFromEnv()

sd, err := libsd.New(cfg, libsd.WithLogger(logger))
if err != nil {
    return err
}
// Stop the background watchers and TTL heartbeats on shutdown so they don't
// leak. Close is idempotent and nil-safe, so a deferred call is always safe.
defer sd.Close()

// Register this service
if err := sd.Register(ctx, libsd.Service{
    ID:   "svc-a-1",
    Name: "svc-a",
    Port: 8081,
    Tags: []string{"v1"},
    HealthCheck: &libsd.HealthCheck{Interval: "10s", Timeout: "3s"},
}); err != nil {
    return err
}

// Resolve a downstream service (static fallback for gradual migration)
addr, err := sd.Resolve(ctx, "svc-b", "svc-b:8082")

// Deregister on shutdown
defer sd.Deregister(ctx, "svc-a-1")
```

**Lifecycle — always `Close()` on shutdown.** The watch-and-cache model runs
background goroutines (**one** watcher per resolved name, plus the TTL heartbeats
started by `Register`). Call `Manager.Close()` on shutdown — `defer sd.Close()`
right after `New`, or from your shutdown hook — to stop every watcher and
heartbeat; skipping it leaks those goroutines for the life of the process.
`Close()` is idempotent and nil-safe, and does **not** deregister services (call
`Deregister` for that).

### Dual endpoints (external / internal)

The endpoint model is **symmetric**: a service can advertise two endpoints for
the same instance, and both are optional `*Endpoint` fields on `Service` — but
**at least one must be advertised to register** (`Register` returns
`ErrNoEndpoint` otherwise). Resolving needs no advertise address at all — see
[Consumer-only Manager](#consumer-only-manager).

- **External** (`Service.External`) — the ingress host, reachable from outside
  the cluster. This is the default view.
- **Internal** (`Service.Internal`) — the in-cluster Kubernetes service DNS
  endpoint (e.g. `svc.ns.svc.cluster.local`), reachable only from inside the
  cluster.

A **provider** announces its external endpoint via `SD_EXTERNAL_ADDRESS` (and
optionally `SD_EXTERNAL_PORT`) and its internal endpoint via `SD_INTERNAL_ADDRESS`
(and optionally `SD_INTERNAL_PORT` / `SD_INTERNAL_SCHEME`). A **consumer** chooses
which endpoint it wants:

```go
// One-shot, explicit view
addr, err := sd.ResolveEndpoint(ctx, "svc-b", libsd.Internal, "svc-b:8082")

// Auto-refreshing resolver with an explicit view
r, err := sd.WatchResolve(ctx, "svc-b", "svc-b:8082", libsd.WithView(libsd.Internal))
```

**Asymmetric resolve contract:**

- **External never receives an internal endpoint.** Asking for `libsd.External`
  against a provider that advertised only an internal endpoint returns
  `ErrEndpointViewUnavailable` — it is never synthesized from the flat fields.
- **Internal degrades safely.** Asking for `libsd.Internal` against a provider
  that never announced an internal endpoint transparently falls back to that
  provider's external endpoint (a warning is logged), so consumers can adopt the
  internal view before every provider has migrated.

The health check registered by `Register` targets the internal endpoint when
`SD_INTERNAL_ADDRESS` is set (Consul runs in-cluster), and the external endpoint
otherwise.

`SD_PREFER_VIEW` sets the default view used by the view-aware resolvers when no
explicit view is passed, and by `ResolvePreferredEndpoint`. The generic
`Resolve` / `ResolveService` methods always return the **root routable** endpoint
(external when advertised, else internal) and are unaffected by this setting.

#### Internal-only deployment

To register a service reachable only inside the cluster, set
`SD_INTERNAL_ADDRESS` (and optionally `SD_INTERNAL_PORT`) and **omit**
`SD_EXTERNAL_ADDRESS` / `SD_ADVERTISE_ADDRESS`. Such a provider has
`Service.External == nil`: `ResolveEndpoint(..., libsd.External, "")` returns
`ErrEndpointViewUnavailable`, while `ResolveEndpoint(..., libsd.Internal, "")` and
the legacy `Resolve` return its internal address.

#### Consumer-only Manager

A service that only **discovers** other services — never registers itself — can
enable discovery with **no advertise address** (omit both `SD_EXTERNAL_ADDRESS`
and `SD_INTERNAL_ADDRESS`). `New` and `Config.Validate` succeed, and the Manager
resolves and watches normally. It simply cannot register: `Register` returns
`ErrNoEndpoint`. This removes the old footgun of setting a dummy advertise address
just to satisfy validation.

```go
// Consumer-only: resolves svc-b, never registers itself.
sd, err := libsd.New(libsd.Config{
    Enabled:    true,
    ConsulAddr: "localhost:8500",
}, libsd.WithLogger(logger))
if err != nil {
    return err
}
defer sd.Close() // stop background watchers on shutdown

// Resolving works (falls back to "svc-b:8082" when discovery is unavailable).
addr, err := sd.Resolve(ctx, "svc-b", "svc-b:8082")
if err != nil {
    return err
}
_ = addr

// Registering would fail — a consumer-only Manager has no advertise address:
//   sd.Register(ctx, svc) → ErrNoEndpoint
```

#### Deprecated flat fields

`Service.Address` / `Port` / `Scheme` are **deprecated**. They are a
one-direction mirror of the root routable endpoint (External when advertised,
else Internal), kept so legacy `Resolve` / `ResolveService` / `Addr` callers
always get a routable address — including for an internal-only provider. They are
**not** a mirror of `External` specifically; `EndpointFor` reads the `External`
pointer directly.

## Environment variables

Canonical names use the backend-agnostic `SD_` prefix. Legacy names are still
accepted as a fallback (`SD_` takes precedence when both are set).

| Variable | Legacy (fallback) | Default | Description |
|---|---|---|---|
| `SD_ENABLED` | `SERVICE_DISCOVERY_ENABLED` | `false` | Set to `"true"` to enable discovery |
| `SD_ADDRESS` | `CONSUL_ADDR` | `localhost:8500` | Discovery server address (host:port) |
| `SD_EXTERNAL_ADDRESS` | `SD_ADVERTISE_ADDRESS`, `SERVICE_ADVERTISE_ADDR` | — | External (ingress) address this service advertises (hostname or full URL). At least one of `SD_EXTERNAL_ADDRESS` / `SD_INTERNAL_ADDRESS` is required to register (a consumer-only Manager needs neither) |
| `SD_EXTERNAL_PORT` | `SD_ADVERTISE_PORT`, `SERVICE_ADVERTISE_PORT` | `0` | External port override (defaults to the port passed to `Register`) |
| `SD_WORKLOAD` | `WORKLOAD_ID` | — | Workload scope for tag-based isolation |
| `SD_TLS` | — | `false` | `"true"` to use HTTPS to the server |
| `SD_TLS_SKIP_VERIFY` | — | `false` | `"true"` to skip server certificate verification |
| `SD_TOKEN` | — | — | ACL token sent to the server |
| `SD_INTERNAL_ADDRESS` | — (no legacy fallback) | `""` | In-cluster (K8s DNS) hostname or full URL for the internal endpoint. Empty = only the external endpoint is advertised |
| `SD_INTERNAL_PORT` | — (no legacy fallback) | `0` | Internal port override (defaults to the port passed to `Register`) |
| `SD_INTERNAL_SCHEME` | — (no legacy fallback) | `""` (`Register` defaults to `http`) | URL scheme for the internal endpoint |
| `SD_PREFER_VIEW` | — (no legacy fallback) | `external` | Default view (`internal`/`external`) for view-aware resolvers and `ResolvePreferredEndpoint` |
| `SD_DIAL_TIMEOUT` | — (no legacy fallback) | `5s` | TCP dial timeout to the server (duration). Bounds connection establishment, not the whole request; never truncates a `Watch` blocking query |
| `SD_TLS_HANDSHAKE_TIMEOUT` | — (no legacy fallback) | `5s` | TLS handshake timeout to the server (duration) |
| `SD_RESPONSE_HEADER_TIMEOUT` | — (no legacy fallback) | `10s` | Response-header timeout applied to the fast (`Register`/`Deregister`/`Resolve`/heartbeat) client **only** — never the `Watch` client, whose blocking queries legitimately withhold headers |
| `SD_SEED_TIMEOUT` | — (no legacy fallback) | `3s` | Bound for the fail-open resolver seed resolve — the `DynamicResolver` seed in `WatchResolve`/`WatchResolveService` and the managed resolvers' lazy seed on the one-shot `Resolve*` path (duration). On timeout the resolver still starts and its background watch converges on the live endpoint |
| `SD_ALLOW_STALE` | — (no legacy fallback) | `true` | `"true"`/`"false"` to opt reads (`Resolve`/`Watch`) into/out of Consul stale mode (`QueryOptions.AllowStale`). **Defaults to `true`** (stale reads) — resolution stays available during a leader blip, at the cost of a possibly slightly stale follower view; set `"false"` for strongly consistent leader reads |

## Running the demo

```bash
make up    # starts consul + svc-a + svc-b + svc-c
make down  # stops and removes containers
```

Then:

```bash
curl http://localhost:8081/ping   # svc-a → svc-b → svc-c chain
curl http://localhost:8081/whoami # shows discovery config
```

## Development

```bash
make test-unit          # unit tests
make test-integration   # requires running Consul
make lint               # golangci-lint
make ci                 # full pipeline
```
