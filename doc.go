// Package libsd provides a service discovery abstraction backed by HashiCorp Consul.
//
// # Overview
//
// The Manager is the single entry point. Its fallback behavior has three modes:
//
//   - Disabled: all operations are no-ops; Resolve returns the fallback address directly.
//   - Enabled with fallback: serves the discovered instance; on a cold cache falls back to a static address.
//   - Enabled without fallback: serves the discovered instance; returns an error when none is cached.
//
// This design allows gradual migration from hardcoded addresses to full service discovery
// without requiring all services to be Consul-aware at once.
//
// A Manager may also run consumer-only: enable discovery with no advertise
// address and it resolves and watches without ever registering itself. The
// advertise requirement applies only to Register (see Config.Validate).
//
// # Resolution model: watch-and-cache
//
// There is ONE resolution pattern, not a query-per-request one. The first resolve
// of a name lazily seeds a per-name resolver (a single bounded Consul read,
// SD_SEED_TIMEOUT) and starts ONE background watch for that name. Every subsequent
// resolve — Resolve, ResolveService, ResolveEndpoint, ResolvePreferredEndpoint —
// reads the cached Service and never contacts Consul on the request path, so a
// slow, dead, or partitioned discovery server can never park a caller. The
// background watch keeps the cache fresh (last-known-good on a transient failure)
// and the seed is fail-open: on seed failure the resolver serves the fallback (or
// errors) while the watch converges on a live value.
//
// One watcher exists per distinct name for the Manager's lifetime; Manager.Close
// stops every watcher and drains the cache (idempotent, nil-safe). Distinct names
// get distinct resolvers; concurrent first-resolves of the same name collapse into
// a single seed.
//
// Because this managed watch-and-cache model runs background goroutines — ONE
// watcher per resolved name plus the TTL heartbeats started by Register — the
// consumer MUST call Manager.Close on shutdown to stop them; otherwise those
// goroutines leak for the lifetime of the process. Close is idempotent and
// nil-safe, so a deferred Close is always safe.
//
// The cache holds ONE instance per name, so consecutive resolves return the same
// address until the catalog changes: this library does NOT load-balance across
// replicas per request. Spreading traffic across a service's pods is the
// downstream's responsibility — resolve to the Kubernetes Service DNS name (which
// distributes across ready pods; an ingress handles the external path), not to an
// individual pod.
//
// # Endpoint views
//
// A registered instance can advertise two endpoints: the external (ingress)
// endpoint and the in-cluster internal (Kubernetes service DNS) endpoint. The
// model is symmetric — both Service.External and Service.Internal are optional,
// but at least one MUST be advertised to REGISTER (Register returns ErrNoEndpoint
// otherwise); resolving needs none (see "Consumer-only Manager" below).
// The EndpointView type selects which one a consumer wants — External (the
// default) or Internal. A provider announces its external endpoint via
// SD_EXTERNAL_ADDRESS (and optionally SD_EXTERNAL_PORT) and its internal endpoint
// via SD_INTERNAL_ADDRESS (and optionally SD_INTERNAL_PORT / SD_INTERNAL_SCHEME).
// A consumer picks a view with Manager.ResolveEndpoint (explicit view) or
// Manager.ResolvePreferredEndpoint (the configured SD_PREFER_VIEW default), and
// the auto-refreshing resolvers accept WithView. Those two return a bare
// host:port; a consumer that builds an HTTP client (a full URL) uses
// Manager.ResolveURL / Manager.ResolvePreferredURL, which return the advertised
// "scheme://host:port" of the view — so switching a consumer external<->internal
// is done purely via SD_PREFER_VIEW, with no per-consumer code change.
//
// The resolve contract is ASYMMETRIC:
//
//   - External never receives an internal endpoint. Asking for External against a
//     provider that advertised only an internal endpoint returns
//     ErrEndpointViewUnavailable — it is never synthesized from the deprecated flat
//     Address/Port/Scheme fields.
//   - Internal degrades safely. Asking for Internal against a provider that never
//     advertised an internal endpoint transparently falls back to its external
//     endpoint (a warning is logged at the resolve layer), so migration is safe.
//
// # Internal-only deployment
//
// A service reachable only inside the cluster can register with just
// SD_INTERNAL_ADDRESS set and SD_EXTERNAL_ADDRESS (and its aliases) omitted. Such
// a provider has Service.External == nil: ResolveEndpoint(..., External, "")
// returns ErrEndpointViewUnavailable, while ResolveEndpoint(..., Internal, "")
// and the legacy Resolve return its internal (root routable) address.
//
// The deprecated flat Service.Address/Port/Scheme fields are a one-direction
// mirror of the root routable endpoint (External when advertised, else Internal),
// kept so legacy Resolve/ResolveService/Addr callers always get a routable
// address — never a mirror of External specifically.
//
// # Consumer-only Manager
//
// A service that only needs to DISCOVER other services — never register itself —
// can enable discovery with no advertise address at all (both SD_EXTERNAL_ADDRESS
// and SD_INTERNAL_ADDRESS omitted). New and Config.Validate succeed; the resulting
// Manager resolves and watches normally. It just cannot register: Register returns
// ErrNoEndpoint because a registrable instance must expose at least one reachable
// endpoint. This removes the old footgun of setting a dummy advertise address only
// to satisfy Validate.
//
// # Usage
//
//	cfg := libsd.ConfigFromEnv()
//	sd, err := libsd.New(cfg, libsd.WithLogger(logger))
//	if err != nil {
//	    return err
//	}
//	defer sd.Close() // stop background watchers + TTL heartbeats on shutdown
//
//	// Register this service
//	if err := sd.Register(ctx, libsd.Service{
//	    ID:   "svc-a-1",
//	    Name: "svc-a",
//	    Port: 8081,
//	    Tags: []string{"v1"},
//	    HealthCheck: &libsd.HealthCheck{Interval: "10s", Timeout: "3s"},
//	}); err != nil {
//	    return err
//	}
//
//	// Resolve a downstream service (with optional static fallback for migration)
//	addr, err := sd.Resolve(ctx, "svc-b", "svc-b:8082")
//
//	// Resolve the in-cluster (internal) endpoint of a downstream service
//	internalAddr, err := sd.ResolveEndpoint(ctx, "svc-b", libsd.Internal, "svc-b:8082")
//
//	// Deregister on shutdown
//	defer sd.Deregister(ctx, "svc-a-1")
//
// # Environment Variables
//
// Canonical, backend-agnostic SD_* names (legacy names accepted as a fallback,
// SD_* taking precedence):
//
//   - SD_ENABLED          — "true" to enable Consul-backed discovery (default: false; legacy: SERVICE_DISCOVERY_ENABLED)
//   - SD_ADDRESS          — discovery server address host:port (default: "localhost:8500"; legacy: CONSUL_ADDR)
//   - SD_EXTERNAL_ADDRESS — hostname or full URL this instance advertises for the external (ingress) view (aliases: SD_ADVERTISE_ADDRESS, SERVICE_ADVERTISE_ADDR)
//   - SD_EXTERNAL_PORT    — external port override, 0 = use the Register port (aliases: SD_ADVERTISE_PORT, SERVICE_ADVERTISE_PORT)
//   - SD_INTERNAL_ADDRESS — in-cluster hostname or full URL for the internal endpoint (no legacy fallback)
//   - SD_INTERNAL_PORT    — internal port override, 0 = use the Register port (no legacy fallback)
//   - SD_INTERNAL_SCHEME  — scheme for the internal endpoint (no legacy fallback)
//   - SD_PREFER_VIEW      — default view for view-aware resolvers: internal/external, default external (no legacy fallback)
//   - SD_WORKLOAD         — workload scope for tag filtering (legacy: WORKLOAD_ID)
//   - SD_TLS              — "true" for HTTPS to the server
//   - SD_TLS_SKIP_VERIFY  — "true" to skip server certificate verification
//   - SD_TOKEN            — ACL token sent to the server
//   - SD_DIAL_TIMEOUT             — TCP dial timeout to the server (duration, e.g. "5s"); empty applies a default in New()
//   - SD_TLS_HANDSHAKE_TIMEOUT    — TLS handshake timeout to the server (duration); empty applies a default in New()
//   - SD_RESPONSE_HEADER_TIMEOUT  — response-header timeout for the fast client only, never the Watch client (duration); empty applies a default in New()
//   - SD_SEED_TIMEOUT             — bound for the fail-open resolver seed resolve, both DynamicResolver and managed (duration); empty applies a default in New()
//   - SD_ALLOW_STALE              — "true"/"false" to opt reads into/out of Consul stale mode; unset defaults to true (stale reads, available during a leader blip)
package libsd
