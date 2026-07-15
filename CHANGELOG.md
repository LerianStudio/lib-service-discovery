# Lib-service-discovery Changelog

## [0.6.0](https://github.com/LerianStudio/lib-service-discovery/releases/tag/v0.6.0)

Features:
- Added `RegisterAsync` method for non-blocking service registration with retries. (@guimoreirar)
- Implemented self-healing mechanisms for TTL checks in Consul. (@guimoreirar)
- Created unit tests for Consul service registration and resolution. (@guimoreirar)
- Developed a Dockerfile for demo services using a multi-stage build process. (@guimoreirar)
- Introduced a `docker-compose` setup for local Consul and service chain demonstrations. (@guimoreirar)

Improvements:
- Enhanced CI workflows with new configurations and coverage filtering. (@guimoreirar)

[Compare changes](https://github.com/LerianStudio/lib-service-discovery/compare/v0.5.0...v0.6.0)

---

## [Unreleased]

### Added
- **Connection hardening**: a tuned HTTP transport bounds connection
  establishment against a dead/slow single-node Consul without ever truncating a
  `Watch` blocking query. The fast client (`Register`/`Deregister`/`Resolve`/
  heartbeat) carries a `ResponseHeaderTimeout`; the watch client deliberately
  omits it so long-poll blocking queries survive. New knobs `DialTimeout`,
  `TLSHandshakeTimeout`, `ResponseHeaderTimeout` (envs `SD_DIAL_TIMEOUT`,
  `SD_TLS_HANDSHAKE_TIMEOUT`, `SD_RESPONSE_HEADER_TIMEOUT`; defaults 5s / 5s /
  10s, applied by `New()`). A **safe TTL floor** raises any TTL-mode health-check
  TTL below 15s up to 15s (and defaults an empty TTL to 30s) so a GC pause or
  brief blip never triggers a false deregistration; an unparseable TTL is
  rejected from `Register` with the new `ErrInvalidTTL` sentinel. **`AllowStale`**
  (`SD_ALLOW_STALE`, a `*bool`; **defaults to `true`** — see **Changed**) routes
  reads (`Resolve`/`Watch`) through Consul stale mode so resolution stays
  available during a leader blip; set `SD_ALLOW_STALE=false` for strong reads.
- **Managed watch-and-cache resolution (single pattern)**: the one-shot resolvers
  (`Resolve`/`ResolveService`/`ResolveEndpoint`/`ResolvePreferredEndpoint`) are now
  backed by a per-name managed resolver. The first resolve of a name lazily seeds
  it (one bounded Consul read, `SD_SEED_TIMEOUT`) and starts **one** background
  watch; every subsequent resolve reads the **cached** `Service` and **never
  contacts Consul on the request path**, so a slow/dead/partitioned discovery
  server can no longer park a caller. The watch keeps the cache fresh
  (last-known-good on a transient failure); the seed is fail-open (serves the
  fallback until the watch converges). Concurrent first-resolves of the same name
  collapse into a single seed; distinct names get distinct resolvers. `SeedTimeout`
  (`SD_SEED_TIMEOUT`) is the **only** bound applied to the seed — the request path
  is unbounded because it is a pure in-memory cache read.
- **Goroutine budget**: exactly one background watcher per distinct resolved name
  for the Manager's lifetime, all derived from a single Manager-lifetime context.
  `Manager.Close()` cancels that context to stop **every** watcher at once (even a
  watch born from a seed still in flight during `Close`), drains the resolver cache,
  and delegates to the registry's `Close()`; it is idempotent and nil-safe. A
  resolve issued **after** `Close` degrades to the fallback and never resurrects a
  watch. The background goroutines (the managed watch loop and `RegisterAsync`) are
  wrapped with lib-observability `runtime.SafeGo` (`KeepRunning`), so a panic is
  logged with its stack and terminates only the goroutine, never the host process.

### Removed
- **Query-per-request resolution removed**: the one-shot resolvers no longer query
  Consul on every call. They are served exclusively by the managed watch-and-cache
  layer above, so the request path is a pure in-memory read.
- **`WithResolveCache` (GAP-3) removed**: the opt-in resolve cache is gone. Caching
  is no longer optional — the managed watch-and-cache layer is now the single,
  always-on resolution model, so a separate cache option is redundant.
- **`Config.ResolveTimeout` / `SD_RESOLVE_TIMEOUT` (GAP-4) removed**: a one-shot
  resolve deadline is unnecessary now that the request path never contacts Consul
  (nothing to time out). `SeedTimeout` (`SD_SEED_TIMEOUT`) is the **only** bound on
  the one Consul read the seed performs; the `Watch` long-poll keeps its own
  lifetime and is never truncated.

### Notes
- **No client-side load balancing (by design)**: the cache holds **one** instance
  per name, so consecutive resolves return the same address until the catalog
  changes. Balancing across replicas is delegated to the platform — resolve to the
  Kubernetes Service DNS name (which distributes across ready pods; an ingress
  handles the external path), not to an individual pod. The internal refresh cursor
  still selects the instance at seed/refresh time. `AllowStale` (`SD_ALLOW_STALE`,
  default `true`) preserves the stale-read semantics described above.

### Changed
- **`AllowStale` now defaults to `true`** (stale reads). The field type changed
  from `bool` to `*bool` so the unset state (nil → default true) is
  distinguishable from an explicit `false`. Resolution now stays available during
  a leader blip by default; set `SD_ALLOW_STALE=false` (or `Config.AllowStale` to
  a pointer to `false`) to restore strongly consistent leader reads. Both
  `AllowStale` and `SD_ALLOW_STALE` are new this cycle and unreleased, so this is
  not a breaking change to any published API.
- **Symmetric dual endpoints (external / internal)**: `Service.External *Endpoint`
  and `Service.Internal *Endpoint` — both optional, at least one required — plus
  the `Endpoint` type (`Address`/`Port`/`Scheme` with `Addr()`) and the
  `EndpointView` type (`External` / `Internal`). External is the ingress host;
  Internal is the in-cluster Kubernetes service DNS endpoint. Either may be
  advertised alone, enabling an **internal-only deployment** (no ingress).
- **`SD_INTERNAL_ADDRESS` / `SD_INTERNAL_PORT` / `SD_INTERNAL_SCHEME`**: providers
  announce a distinct in-cluster endpoint (no legacy fallback). The internal
  endpoint is serialized to Consul Meta under `internal_address` /
  `internal_port` / `internal_scheme`.
- **`Manager.ResolveEndpoint(ctx, name, view, fallback)`**: resolve the external
  or internal endpoint explicitly, following the asymmetric resolve contract
  (see **Changed**).
- **Health check targets the internal endpoint** when `SD_INTERNAL_ADDRESS` is
  set (Consul runs in-cluster), and the external endpoint otherwise.
- **View-aware resolvers**: `DynamicResolver` now carries a view, and
  `WatchResolve` / `WatchResolveService` accept `WithView(view)` to track the
  external or internal endpoint.
- **`SD_PREFER_VIEW`** (`internal` / `external`, default `external`): the default
  view used by the view-aware resolvers when no explicit view is passed, and by
  `ResolvePreferredEndpoint`.
- **`Manager.ResolvePreferredEndpoint(ctx, name, fallback)`**: resolve using the
  configured default view (`SD_PREFER_VIEW`) without threading a view through.
- **`SD_EXTERNAL_ADDRESS` / `SD_EXTERNAL_PORT`**: preferred names for the external
  endpoint. `SD_ADVERTISE_ADDRESS` / `SD_ADVERTISE_PORT` / `SERVICE_ADVERTISE_ADDR`
  remain accepted as back-compat aliases (`SD_EXTERNAL_*` takes precedence).
- **`ErrNoEndpoint`** sentinel, returned by `Validate` when discovery is enabled
  but no endpoint (external or internal) is configured.

### Changed
- **BREAKING — `Service.EndpointFor` signature**: now returns `(Endpoint, error)`
  instead of a single `Endpoint`. Callers must handle the error
  (`ErrEndpointViewUnavailable`).
- **BREAKING — `Validate` requires at least one endpoint**: discovery-enabled
  configs must set an external (`AdvertiseAddr`) **or** an internal
  (`AdvertiseInternalAddr`) address; neither returns the new `ErrNoEndpoint`. The
  external address is no longer mandatory on its own (`ErrEmptyAdvertiseAddr` is
  deprecated in favor of `ErrNoEndpoint`).
- **BREAKING — symmetric endpoint model**: `Service.External *Endpoint` is added
  alongside `Service.Internal`. The flat `Service.Address` / `Port` / `Scheme`
  fields are **deprecated**: they are now a one-direction mirror of the *root
  routable* endpoint (External when advertised, otherwise Internal), preserved so
  legacy `Resolve` / `ResolveService` / `Addr` callers always get a routable
  address — including for an internal-only provider. They are **not** a mirror of
  `External` specifically; `EndpointFor` reads the `External` pointer directly and
  never synthesizes an external view from the flat fields.
- **BREAKING — Consul Meta serialization**: the external endpoint is now
  serialized under `external_address` / `external_port` / `external_scheme` (the
  `scheme` key is still mirrored for back-compat) in addition to the
  `internal_*` keys.
- **Asymmetric resolve contract**: requesting the `External` view against a
  provider that advertised only an internal endpoint returns
  `ErrEndpointViewUnavailable` (no synthesis from the flat mirror). Requesting the
  `Internal` view against a provider with no internal endpoint degrades to its
  external endpoint **without error**, logging a warning at the resolve layer.
- `Resolve` / `ResolveService` are unchanged for external-advertising providers;
  against an internal-only provider they now return that provider's internal
  (root routable) address rather than an empty `":0"`.

## [v0.6.0] - 2026-06-30

### Added
- **`Manager.RegisterAsync`**: non-blocking registration that retries in the
  background with exponential backoff until it succeeds or the context is
  cancelled. Startup no longer depends on the discovery server being reachable.
- **Self-healing TTL heartbeat**: when a heartbeat finds its check unknown
  (HTTP 404 — the registration was lost after a Consul restart, or dropped by a
  server-only/agentless catalog), the registry re-registers the service to
  recreate the check and resumes, instead of failing every TTL/2 forever.
- **Unit coverage for the Consul backend**: the wire paths (register, resolve,
  watch, TTL heartbeat, deregister, and the self-heal path) are now exercised
  against an `httptest` fake of the Consul HTTP API, so they run in CI without a
  live Consul agent.

### Fixed
- Reconciled this changelog with the repository's real tags (`v0.1.0`–`v0.5.0`).
  Prior entries (`v1.x`/`v2.x`, dated 2025-07) were release-template residue and
  did not correspond to any tag.

## [v0.5.0] - 2026-06-24

### Changed
- Standardized environment variables under the backend-agnostic `SD_` prefix
  (`SD_ENABLED`, `SD_ADDRESS`, `SD_ADVERTISE_ADDRESS`, `SD_ADVERTISE_PORT`,
  `SD_WORKLOAD`). Legacy names (`SERVICE_DISCOVERY_ENABLED`, `CONSUL_ADDR`,
  `SERVICE_ADVERTISE_ADDR`/`_PORT`, `WORKLOAD_ID`) remain accepted as a fallback,
  with `SD_*` taking precedence.

### Added
- `SD_TLS`, `SD_TLS_SKIP_VERIFY` and `SD_TOKEN` for HTTPS and ACL-protected
  discovery servers.

## [v0.4.0] - 2026-06-24

### Added
- `Resolve`/`ResolveService` log their outcome at Info with an explicit `source`
  field (`consul` or `fallback`) for clearer operational signal.

## [v0.3.0] - 2026-05-19

### Added
- Scheme is parsed from `SERVICE_ADVERTISE_ADDR` (e.g. `https://host`) and
  propagated to Consul via service Meta, so resolvers recover the URL scheme.

## [v0.2.0] - 2026-05-19

### Added
- `Scheme` field on `Service` and the `ResolveService` method, exposing the full
  resolved service (scheme and metadata) beyond `host:port`.

## [v0.1.1] - 2026-05-19

### Added
- Successful Consul lookups log the resolved address.

## [v0.1.0] - 2026-05-19

### Added
- Initial public release: `Manager` over a Consul `Registry` with `Register`,
  `Resolve`, `Deregister` and `Watch`; three operating modes (disabled,
  enabled-with-fallback, enabled-strict); TTL heartbeat and HTTP health checks;
  workload tag isolation; configuration via environment variables.

[Unreleased]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.6.0...HEAD
[v0.6.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/LerianStudio/lib-service-discovery/releases/tag/v0.1.0

