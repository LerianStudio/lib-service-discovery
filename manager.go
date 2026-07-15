package libsd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	obsruntime "github.com/LerianStudio/lib-observability/runtime"
)

// Manager is the entry point for lib-service-discovery.
//
// When SERVICE_DISCOVERY_ENABLED=false, Register and Deregister are no-ops,
// and Resolve returns the fallback address directly.
//
// When SERVICE_DISCOVERY_ENABLED=true, Register publishes this service to Consul
// and Resolve queries Consul for healthy instances, falling back to the static
// address only when a fallback is provided.
//
// When a workload is configured (via Config.Workload or WithWorkload), Register
// automatically tags services with "workload=<id>" and Resolve filters results
// to instances carrying that tag, isolating each deployment group.
type Manager struct {
	config   Config
	registry Registry
	logger   log.Logger
	workload string
	// seedTimeout bounds both the DynamicResolver's initial (seed) resolve and the
	// managed resolvers' lazy seed. Sourced from Config.SeedTimeout (defaulted by
	// withDefaults). It is applied only to the seed call, on a context derived from
	// the caller's; the long-lived watch keeps its own lifetime context and is
	// never truncated by this deadline.
	seedTimeout time.Duration
	// preferView is the default view supplied to view-aware resolvers when the
	// caller passes no explicit view (WithView). Sourced from Config.PreferView;
	// the zero value "" resolves as External.
	preferView EndpointView
	// resolvers holds one managedResolver per service name, created lazily on the
	// first resolve of that name. Each keeps a background watch that refreshes a
	// cached Service, so the one-shot resolve request path never contacts the
	// discovery server — the single resolution pattern is watch-and-cache.
	// resolversMu guards the map (get-or-create) and closed; the per-resolver seed
	// runs under the resolver's own sync.Once, outside this mutex. It is an RWMutex
	// so steady-state resolves of an already-created name take only a read lock —
	// the hot path never contends on an exclusive lock once a name is seeded.
	resolvers   map[string]*managedResolver
	resolversMu sync.RWMutex
	// closed reports whether Close has run. Guarded by resolversMu (written by
	// Close, read on the first-resolve slow path). Once true, managedResolverFor
	// never creates or registers a new watcher: a post-Close resolve gets a
	// degraded, un-seeded resolver so it falls back rather than resurrecting a watch.
	closed bool
	// baseCtx is the Manager-lifetime context every managed watch derives from;
	// baseCancel cancels it. Close calls baseCancel to tear down all managed watches
	// at once — including any watch born from a seed still in flight during Close
	// (it starts on an already-cancelled context and exits immediately, so it never
	// leaks). Created in New before the enabled check, so Close is always safe.
	baseCtx    context.Context
	baseCancel context.CancelFunc
	// closeOnce guards the shutdown path so the whole teardown (including the
	// registry's Close) runs exactly once; closeErr retains its result so repeated
	// Close calls return the same error without re-invoking a possibly
	// non-idempotent custom registry closer.
	closeOnce sync.Once
	closeErr  error
}

// Option configures a Manager after construction.
type Option func(*Manager)

// WithLogger sets the structured logger used by the Manager and its registry.
// A nil logger is silently ignored; the Config.Logger (or log.NewNop()) is used instead.
func WithLogger(l log.Logger) Option {
	return func(m *Manager) {
		if m == nil || l == nil {
			return
		}

		m.logger = l
	}
}

// WithRegistry overrides the Registry backend used for service discovery.
// Useful for alternative providers (etcd, K8s) or in-memory stubs in tests.
// A nil registry is silently ignored; the default Consul backend is used instead.
func WithRegistry(r Registry) Option {
	return func(m *Manager) {
		if m == nil || r == nil {
			return
		}

		m.registry = r
	}
}

// WithWorkload overrides the workload scope set in Config.Workload.
// An empty string clears the workload filter (all healthy instances match).
func WithWorkload(id string) Option {
	return func(m *Manager) {
		if m == nil {
			return
		}

		m.workload = id
	}
}

// New creates a Manager from cfg. Options are applied before the default Consul
// registry is created, so WithRegistry can override the backend entirely.
//
// New requires only ConsulAddr when discovery is enabled (ErrEmptyConsulAddr
// otherwise). An advertise address is NOT required: an enabled Manager with no
// advertise address is a valid consumer-only Manager that resolves and watches but
// cannot register. The advertise requirement is enforced by Register, which returns
// ErrNoEndpoint when the service to register has no endpoint.
func New(cfg Config, opts ...Option) (*Manager, error) {
	cfg = cfg.withDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	m := &Manager{
		config:      cfg,
		logger:      cfg.Logger,
		workload:    cfg.Workload,
		preferView:  cfg.PreferView,
		seedTimeout: cfg.SeedTimeout,
	}

	// The Manager-lifetime context every managed watch derives from. Cancelled by
	// Close so all background watches stop at once. Set before the enabled check so
	// even a disabled Manager has a valid baseCancel and Close stays safe.
	m.baseCtx, m.baseCancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}

	if !cfg.Enabled {
		m.logger.Log(context.Background(), log.LevelInfo,
			"service discovery disabled — using direct addresses")

		return m, nil
	}

	if m.registry == nil {
		registry, err := newConsulRegistry(cfg, m.logger)
		if err != nil {
			return nil, fmt.Errorf("lib-service-discovery: %w", err)
		}

		m.registry = registry

		m.logger.Log(context.Background(), log.LevelInfo, "service discovery enabled",
			log.String("consul", cfg.ConsulAddr),
			log.String("advertise", cfg.AdvertiseAddr))
	} else {
		m.logger.Log(context.Background(), log.LevelInfo, "service discovery enabled",
			log.String("registry", "custom"),
			log.String("advertise", cfg.AdvertiseAddr))
	}

	return m, nil
}

// Register publishes svc to the service registry using the configured AdvertiseAddr.
// When a workload is set, it is appended to svc.Tags as "workload=<id>" so that
// Resolve calls from managers with the same workload find this instance.
// No-op when discovery is disabled.
func (m *Manager) Register(ctx context.Context, svc Service) error {
	if m == nil {
		return ErrNilManager
	}

	if !m.config.Enabled {
		return nil
	}

	// The caller's flat Address/Port/Scheme are raw inputs; the advertised
	// endpoints are derived entirely from config below. Capture the Register port
	// (a port default) and scheme (external-scheme precedence), then clear the flat
	// mirror so normalizeEndpoints cannot resurrect a stale external endpoint from
	// a leftover port on an internal-only registration.
	callerPort := svc.Port
	callerScheme := svc.Scheme
	svc.Address = ""
	svc.Port = 0
	svc.Scheme = ""
	// Also drop the caller's endpoint pointers: the library derives External and
	// Internal SOLELY from config below. Otherwise a caller-supplied External could
	// survive an internal-only config and publish an unauthorized external endpoint
	// (and vice versa).
	svc.External = nil
	svc.Internal = nil

	// The external port is the advertised override when set, else the caller's
	// Register port. It also feeds the internal-port default below.
	extPort := callerPort
	if m.config.AdvertisePort > 0 {
		extPort = m.config.AdvertisePort
	}

	// Build the external endpoint from config, ONLY when an external address is
	// advertised. The caller's scheme wins over the configured scheme.
	if m.config.AdvertiseAddr != "" {
		scheme := callerScheme
		if scheme == "" {
			scheme = m.config.AdvertiseScheme
		}

		svc.External = &Endpoint{Address: m.config.AdvertiseAddr, Port: extPort, Scheme: scheme}
	}

	// Populate the in-cluster (K8s DNS) endpoint from config when advertised.
	// Port defaults to the advertised external port when an external endpoint is
	// present, else the caller's Register port. Scheme defaults to "http":
	// in-cluster traffic is plaintext by default (TLS terminates at the ingress).
	if m.config.AdvertiseInternalAddr != "" {
		port := m.config.AdvertiseInternalPort
		if port == 0 {
			if m.config.AdvertiseAddr != "" {
				port = extPort // default: the advertised external port
			} else {
				port = callerPort // no external endpoint: the caller's Register port
			}
		}

		scheme := m.config.AdvertiseInternalScheme
		if scheme == "" {
			scheme = "http" // in-cluster traffic is plaintext by default (TLS terminates at ingress)
		}

		svc.Internal = &Endpoint{Address: m.config.AdvertiseInternalAddr, Port: port, Scheme: scheme}
	}

	// Reconcile External <-> the deprecated flat mirror after config is applied.
	svc.normalizeEndpoints()

	// Registering requires a reachable endpoint. The advertised endpoints are
	// derived solely from config above (the caller's flat fields and endpoint
	// pointers were cleared), so a consumer-only Manager (Enabled with no advertise
	// address) reaches here with neither External nor Internal set. Resolving does
	// not need an endpoint, but registering does — surface ErrNoEndpoint rather than
	// publishing an instance with a ":0" address. This is the requirement that used
	// to live in Config.Validate; it now applies only on the register path.
	if svc.External == nil && svc.Internal == nil {
		return ErrNoEndpoint
	}

	if m.workload != "" {
		// Copy before appending so we never mutate the caller's backing array.
		tags := make([]string, len(svc.Tags), len(svc.Tags)+1)
		copy(tags, svc.Tags)
		svc.Tags = append(tags, "workload="+m.workload)
	}

	svc, err := m.applyHealthCheck(ctx, svc)
	if err != nil {
		return err
	}

	return m.registry.Register(ctx, svc)
}

// applyHealthCheck normalizes svc.HealthCheck ahead of registration and returns the
// updated Service. It never mutates the caller's HealthCheck (svc is a value but its
// HealthCheck is a shared pointer, so it copies before writing). TTL checks need no
// reachable endpoint; only HTTP checks build a probe URL. An unparseable TTL is a
// hard configuration error (ErrInvalidTTL). Callers pass a svc that already has at
// least one endpoint (Register enforces ErrNoEndpoint first).
func (m *Manager) applyHealthCheck(ctx context.Context, svc Service) (Service, error) {
	switch {
	case svc.HealthCheck != nil && svc.HealthCheck.TTL != "":
		// TTL mode: normalize the TTL through the safe floor before it reaches the
		// registry, so a GC pause or brief blip never triggers a false
		// deregistration.
		ttl, err := ttlWithDefaults(svc.HealthCheck.TTL)
		if err != nil {
			return svc, err
		}

		hc := *svc.HealthCheck
		hc.TTL = ttl
		svc.HealthCheck = &hc

	case svc.HealthCheck != nil:
		// HTTP mode (TTL empty).
		hc := *svc.HealthCheck

		// Probe the internal endpoint when advertised (Consul runs in-cluster),
		// degrading to the external endpoint otherwise. EndpointFor(Internal) only
		// errors when neither endpoint exists — defensive, since Register already
		// rejected the no-endpoint case; in that case skip the HTTP URL.
		if target, err := svc.EndpointFor(Internal); err != nil {
			m.logger.Log(ctx, log.LevelWarn, "no reachable endpoint for health check",
				log.String("name", svc.Name))
		} else {
			hc.HTTP = healthCheckURL(target.Scheme, target.Address, target.Port, hc.Path)
		}

		svc.HealthCheck = &hc
	}

	return svc, nil
}

// RegisterAsync registers svc without blocking the caller and without failing
// startup: it retries in the background with exponential backoff until Register
// succeeds or ctx is cancelled. Use it when the service must come up even if the
// discovery server is briefly unavailable at boot — boot no longer depends on
// Consul being reachable. Pass an app-lifetime ctx (the one cancelled on
// shutdown), not a request-scoped one. No-op when discovery is disabled.
func (m *Manager) RegisterAsync(ctx context.Context, svc Service) {
	if m == nil || !m.config.Enabled {
		return
	}

	// SafeGo wraps the retry loop with panic recovery (KeepRunning): a panic in
	// this background goroutine is logged with its stack and terminates only the
	// goroutine, never the host process. A bare `go` would let any panic (e.g. a
	// nil deref inside a custom Registry) crash the whole service.
	obsruntime.SafeGo(m.logger, "libsd.register-async:"+svc.Name, obsruntime.KeepRunning, func() {
		for attempt := 0; ; attempt++ {
			// Stop immediately when the Manager closes, even if the caller ctx is
			// still live: a retry that lands after Close would register (and, on the
			// Consul backend, start a heartbeat) that escapes Close's cleanup.
			if m.baseCtx.Err() != nil {
				return
			}

			err := m.Register(ctx, svc)
			if err == nil {
				if attempt > 0 {
					m.logger.Log(ctx, log.LevelInfo, "service registered after retry",
						log.String("name", svc.Name),
						log.Int("attempts", attempt))
				}

				return
			}

			m.logger.Log(ctx, log.LevelWarn, "register attempt failed; will retry",
				log.String("name", svc.Name),
				log.Int("attempt", attempt),
				log.Err(err))

			// Wake on the caller ctx OR the Manager-lifetime baseCtx, so Close aborts
			// a pending backoff at once.
			if !sleepCtxAny(ctx, m.baseCtx, backoffDuration(attempt)) {
				return
			}
		}
	})
}

// healthCheckURL builds the URL Consul probes for an HTTP health check, honoring
// the service scheme (defaulting to "http") and path (defaulting to "/health").
func healthCheckURL(scheme, addr string, port int, path string) string {
	if scheme == "" {
		scheme = "http"
	}

	if path == "" {
		path = "/health"
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return fmt.Sprintf("%s://%s:%d%s", scheme, addr, port, path)
}

// workloadTag returns the "workload=<id>" resolve filter for the configured
// workload, or "" when no workload is set (match any instance).
func (m *Manager) workloadTag() string {
	if m.workload == "" {
		return ""
	}

	return "workload=" + m.workload
}

// resolveEmptyFallback applies the fail-open precedence when the managed cache is
// empty (the lazy seed failed, the background watch has not yet recovered a value,
// or an authoritative empty catalog cleared it): serve the per-call fallback when
// provided, else surface the resolver's empty error (mr.emptyErr) — the
// authoritative ErrNoHealthyInstances after a clear, otherwise the seed error, else
// ErrNoHealthyInstances.
func (m *Manager) resolveEmptyFallback(ctx context.Context, name, fallback string, mr *managedResolver) (string, error) {
	emptyErr := mr.emptyErr()

	if fallback != "" {
		m.logger.Log(ctx, log.LevelWarn, "service resolved",
			log.String("service", name),
			log.String("addr", fallback),
			log.String("source", "fallback"),
			log.String("reason", "discovery unavailable"),
			log.Err(emptyErr))

		return fallback, nil
	}

	return "", emptyErr
}

// Resolve returns the host:port address of name.
//
//   - Discovery disabled: returns fallback, or ErrDiscoveryDisabledNoFallback when empty.
//   - Discovery enabled: the name's managed resolver is created (and lazily seeded)
//     on first use; every call reads the cached Service refreshed by a background
//     watch, so Consul is never contacted on the request path.
//   - Enabled, cache populated: returns the cached instance address (filtered by
//     workload tag when a workload is configured).
//   - Enabled, cache empty (seed failed, watch not yet recovered), fallback provided:
//     returns fallback (logs warning).
//   - Enabled, cache empty, no fallback: returns ErrNoHealthyInstances when the
//     emptiness is authoritative (catalog confirmed empty), else the seed error.
//
// To be fail-open when the discovery server is down, provide a fallback: while the
// lazy seed fails the resolver serves it until the watch recovers a live value.
//
// Resolve returns ONE cached instance address and does not balance across replicas
// per request — consecutive calls return the same address until the catalog
// changes. Spreading load across a service's pods is the downstream's job: resolve
// to the Kubernetes Service name (which load-balances across ready pods; an ingress
// handles the external path), not to an individual pod.
func (m *Manager) Resolve(ctx context.Context, name, fallback string) (string, error) {
	if m == nil {
		return "", ErrNilManager
	}

	if !m.config.Enabled {
		if fallback == "" {
			return "", fmt.Errorf("%w: %q", ErrDiscoveryDisabledNoFallback, name)
		}

		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", fallback),
			log.String("source", "fallback"),
			log.String("reason", "discovery disabled"))

		return fallback, nil
	}

	mr := m.managedResolverFor(ctx, name)

	if svc, ok := mr.service(); ok {
		return svc.Addr(), nil
	}

	return m.resolveEmptyFallback(ctx, name, fallback, mr)
}

// ResolveService is like Resolve but returns the full Service struct, giving
// callers access to Scheme and other fields beyond host:port. It is served by the
// same managed watch-and-cache layer: the cached Service (or fallback) is returned
// without contacting Consul on the request path. An empty fallback.Address is
// treated as "no fallback". Like Resolve, it returns ONE cached instance and does
// no client-side per-request balancing — spreading load across replicas is the
// downstream's job (resolve to the Kubernetes Service name).
func (m *Manager) ResolveService(ctx context.Context, name string, fallback Service) (Service, error) {
	if m == nil {
		return Service{}, ErrNilManager
	}

	if !m.config.Enabled {
		if fallback.Address == "" {
			return Service{}, fmt.Errorf("%w: %q", ErrDiscoveryDisabledNoFallback, name)
		}

		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", fallback.Addr()),
			log.String("source", "fallback"),
			log.String("reason", "discovery disabled"))

		return fallback, nil
	}

	mr := m.managedResolverFor(ctx, name)

	if svc, ok := mr.service(); ok {
		return svc, nil
	}

	emptyErr := mr.emptyErr()

	if fallback.Address != "" {
		m.logger.Log(ctx, log.LevelWarn, "service resolved",
			log.String("service", name),
			log.String("addr", fallback.Addr()),
			log.String("source", "fallback"),
			log.String("reason", "discovery unavailable"),
			log.Err(emptyErr))

		return fallback, nil
	}

	return Service{}, emptyErr
}

// ResolveEndpoint resolves name and returns the host:port of the requested view
// (External = ingress; Internal = in-cluster K8s DNS). Same disabled/fallback
// semantics as Resolve, served by the managed watch-and-cache layer: the cached
// Service is mapped through view on the request path (no Consul call). fallback is
// a static host:port independent of view.
//
// The External view against an internal-only provider surfaces
// ErrEndpointViewUnavailable (unless a fallback is given); the Internal view
// against an external-only provider degrades to the external endpoint with a
// warning.
//
// Like Resolve, it returns ONE cached instance per name and does no client-side
// per-request balancing — spreading load across replicas is the downstream's job
// (resolve to the Kubernetes Service name; an ingress handles the external path).
func (m *Manager) ResolveEndpoint(ctx context.Context, name string, view EndpointView, fallback string) (string, error) {
	if m == nil {
		return "", ErrNilManager
	}

	if !m.config.Enabled {
		if fallback == "" {
			return "", fmt.Errorf("%w: %q", ErrDiscoveryDisabledNoFallback, name)
		}

		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", fallback),
			log.String("source", "fallback"),
			log.String("reason", "discovery disabled"))

		return fallback, nil
	}

	mr := m.managedResolverFor(ctx, name)

	svc, ok := mr.service()
	if !ok {
		return m.resolveEmptyFallback(ctx, name, fallback, mr)
	}

	ep, epErr := svc.EndpointFor(view)
	if epErr != nil {
		// The requested view is unavailable (e.g. External view against an
		// internal-only provider). Treat as a miss: use the fallback when provided,
		// otherwise surface the view error.
		if fallback != "" {
			m.logger.Log(ctx, log.LevelWarn, "service resolved",
				log.String("service", name),
				log.String("addr", fallback),
				log.String("source", "fallback"),
				log.String("reason", "requested view unavailable"),
				log.String("view", string(view)),
				log.Err(epErr))

			return fallback, nil
		}

		return "", epErr
	}

	// Internal view satisfied by degrading to the external endpoint: warn so
	// operators can see the provider never advertised an internal endpoint.
	if view == Internal && svc.Internal == nil {
		m.logger.Log(ctx, log.LevelWarn, "internal view degraded to external",
			log.String("service", name),
			log.String("addr", ep.Addr()),
			log.String("view", string(view)))
	}

	return ep.Addr(), nil
}

// ResolvePreferredEndpoint resolves name using the Manager's configured default
// view (Config.PreferView / SD_PREFER_VIEW), returning the host:port of that
// view. It is a one-shot, opt-in convenience over ResolveEndpoint for callers
// that want the configured default without threading the view through
// themselves; the generic Resolve/ResolveService/ResolveEndpoint remain
// view-explicit and are unaffected. Same disabled/fallback semantics as
// ResolveEndpoint: fallback is a static host:port independent of view.
func (m *Manager) ResolvePreferredEndpoint(ctx context.Context, name, fallback string) (string, error) {
	if m == nil {
		return "", ErrNilManager
	}

	return m.ResolveEndpoint(ctx, name, m.preferView, fallback)
}

// Deregister removes serviceID from the registry.
// No-op when discovery is disabled.
func (m *Manager) Deregister(ctx context.Context, serviceID string) error {
	if m == nil {
		return ErrNilManager
	}

	if !m.config.Enabled {
		return nil
	}

	return m.registry.Deregister(ctx, serviceID)
}

// Close releases background resources held by the Manager — chiefly the TTL
// heartbeat goroutines started by Register/RegisterAsync — so a consumer that
// forgets to Deregister does not leak them. It delegates to the registry when
// the backend supports shutdown (the Consul backend does); a backend that does
// not is a safe no-op.
//
// Close is idempotent (safe to call multiple times) and nil-receiver safe.
// After Close the Manager should not be reused: heartbeats are stopped and the
// resolvers degrade to fallback/error rather than a live registry. It does not
// deregister services from Consul — call Deregister for that; Close only stops
// the local goroutines.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}

	// Run the teardown exactly once and retain its result, so a repeated Close
	// never re-invokes a non-idempotent custom registry closer and every caller
	// observes the same error.
	m.closeOnce.Do(func() {
		m.closeErr = m.doClose()
	})

	return m.closeErr
}

// doClose performs the one-time Manager teardown behind Close's sync.Once.
func (m *Manager) doClose() error {
	// Mark closed and cancel the base context BEFORE draining, all under the same
	// lock, so a first-resolve running concurrently either (a) sees closed and never
	// creates a watcher, or (b) already created one whose watch derives from the now
	// cancelled base context and therefore exits at once. Cancelling the base context
	// also tears down a watch born from a seed still in flight during Close: it starts
	// on the cancelled context and returns immediately — no orphaned goroutine.
	m.resolversMu.Lock()
	m.closed = true

	if m.baseCancel != nil {
		m.baseCancel()
	}

	// Stop every managed resolver's background watch and drain the map, so a
	// consumer that resolved names does not leak the watch goroutines. Draining
	// makes Close idempotent (a second call finds an empty map).
	for name, mr := range m.resolvers {
		mr.stop()
		delete(m.resolvers, name)
	}
	m.resolversMu.Unlock()

	if m.registry == nil {
		return nil
	}

	// Delegate via an optional Close() seam so custom Registry backends and test
	// stubs that do not implement it are unaffected (no interface change).
	if closer, ok := m.registry.(interface{ Close() error }); ok {
		return closer.Close()
	}

	return nil
}

// Watch returns a channel that emits events when the health state of name changes.
// Returns a closed channel (no error) when discovery is disabled.
func (m *Manager) Watch(ctx context.Context, name string) (<-chan Event, error) {
	if m == nil {
		ch := make(chan Event)
		close(ch)

		return ch, ErrNilManager
	}

	if !m.config.Enabled {
		ch := make(chan Event)
		close(ch)

		return ch, nil
	}

	return m.registry.Watch(ctx, name)
}
