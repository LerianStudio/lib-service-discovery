package libsd

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/LerianStudio/lib-observability/log"
	obsruntime "github.com/LerianStudio/lib-observability/runtime"
)

// managedResolver is the per-name watch-and-cache state that backs the one-shot
// resolvers (Resolve/ResolveService/ResolveEndpoint/ResolvePreferredEndpoint).
//
// It holds the full last-known-good Service in an atomic value, seeded once
// (lazily, on first use) and thereafter refreshed only by a background watch. The
// request path reads the cached Service and never contacts the discovery server,
// so a slow, dead, or partitioned discovery server can never park a caller: the
// single resolution pattern is watch-and-cache. Consul is touched only by the
// background watcher plus one lazy seed per name (fail-open).
//
// The cache holds ONE instance per name (the one the seed/refresh cursor picked),
// so consecutive resolves of the same name return the same address: this layer
// does NOT balance across replicas per request. Client-side load balancing is
// intentionally out of scope — spreading traffic across a service's pods is the
// downstream's job (the Kubernetes Service DNS distributes across ready pods; an
// ingress handles the external path). Resolve to the Service name and let the
// platform fan out.
type managedResolver struct {
	// current holds the last-known-good Service. It is written by the lazy seed
	// and by the background watch, and read by every resolve; atomic.Value keeps
	// those accesses race-free without locking the request path.
	current atomic.Value // Service
	// once guards the lazy seed + watch start, so concurrent first-resolves of the
	// same name collapse into a single seed instead of each hitting the registry.
	once sync.Once
	// seeded reports whether current holds a usable Service. It is set true on
	// every successful store (seed or watch) and always read BEFORE current, so a
	// reader that observes seeded==true is guaranteed (Go's sync/atomic operations
	// are sequentially consistent) to observe the Service stored before it.
	seeded atomic.Bool
	// seedErr records the lazy seed's failure, if any. It is written only inside
	// once (single writer) and read only when the cache is empty; the once/atomic
	// happens-before covers it without further synchronization.
	seedErr error
	// cancel stops the background watch. It is an atomic pointer because the seed
	// (which sets it, outside resolversMu) and Manager.Close (which reads it, under
	// resolversMu) can run concurrently for a first-resolve still in flight.
	cancel atomic.Pointer[context.CancelFunc]
}

// store records svc as the last-known-good value and marks the resolver seeded.
// current is written before seeded so a reader gating on seeded never observes an
// absent or stale Service.
func (mr *managedResolver) store(svc Service) {
	mr.current.Store(svc)
	mr.seeded.Store(true)
}

// service returns the cached Service and whether one is present.
func (mr *managedResolver) service() (Service, bool) {
	if !mr.seeded.Load() {
		return Service{}, false
	}

	svc, _ := mr.current.Load().(Service)

	return svc, true
}

// stop cancels the background watch. Safe when no watch was ever started.
func (mr *managedResolver) stop() {
	if fn := mr.cancel.Load(); fn != nil {
		(*fn)()
	}
}

// managedResolverFor returns the managedResolver for name, creating and seeding it
// on first use. The hot path (a name that already has a resolver) takes only a
// read lock, so steady-state resolves never contend on an exclusive lock. Only the
// first-ever resolve of a name takes the write lock to insert the resolver. The
// seed runs under the resolver's sync.Once OUTSIDE the lock, so a slow seed for one
// name never blocks resolves of another, while concurrent first-resolves of the
// SAME name collapse into a single seed.
//
// After Close the write path sees closed and returns a degraded, un-seeded resolver
// WITHOUT registering it or starting a watch, so a resolve issued post-Close falls
// back (or errors) instead of resurrecting a background watch.
func (m *Manager) managedResolverFor(ctx context.Context, name string) *managedResolver {
	// Fast path: once a name's resolver exists, a read lock suffices. This is the
	// steady state (post-seed), so the hot request path never takes an exclusive lock.
	m.resolversMu.RLock()
	mr, ok := m.resolvers[name]
	m.resolversMu.RUnlock()

	if ok {
		mr.once.Do(func() {
			m.seedManagedResolver(ctx, mr, name)
		})

		return mr
	}

	// Slow path: first resolve of this name. Take the write lock and re-check —
	// another goroutine may have created it between the RUnlock and the Lock.
	m.resolversMu.Lock()

	if m.closed {
		// Closed: never create or register a new watcher. Return an ephemeral,
		// un-seeded resolver (not stored in the map) so the request path degrades to
		// fallback/ErrNoHealthyInstances rather than starting an orphaned watch.
		m.resolversMu.Unlock()

		return &managedResolver{}
	}

	if m.resolvers == nil {
		m.resolvers = make(map[string]*managedResolver)
	}

	mr, ok = m.resolvers[name]
	if !ok {
		mr = &managedResolver{}
		m.resolvers[name] = mr
	}

	m.resolversMu.Unlock()

	mr.once.Do(func() {
		m.seedManagedResolver(ctx, mr, name)
	})

	return mr
}

// seedManagedResolver performs the one-time lazy seed for mr and starts its
// background watch. It is fail-open: a seed error (or timeout) leaves the cache
// empty and is recorded in mr.seedErr, and the watch is started anyway so a later
// catalog change can populate the cache. The seed is bounded by SeedTimeout on a
// context derived from the caller's; the watch keeps its OWN manager-lifetime
// context (see startManagedWatch) so a request-scoped ctx can never tear down the
// shared watch.
func (m *Manager) seedManagedResolver(ctx context.Context, mr *managedResolver, name string) {
	tag := m.workloadTag()

	seedCtx, cancel := context.WithTimeout(ctx, m.seedTimeout)
	svc, err := m.registry.Resolve(seedCtx, name, tag)

	cancel() // release the seed deadline immediately; the watch uses its own context.

	if err != nil {
		mr.seedErr = err
		m.logger.Log(ctx, log.LevelWarn, "managed resolver seed degraded; watching for recovery",
			log.String("service", name),
			log.String("workload", m.workload),
			log.Err(err))
	} else {
		mr.store(svc)
		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", svc.Addr()),
			log.String("source", "consul"),
			log.String("workload", m.workload))
	}

	m.startManagedWatch(mr, name, tag)
}

// startManagedWatch opens the catalog watch for name on the MANAGER-LIFETIME base
// context (m.baseCtx, cancellable via mr.cancel per-resolver or Manager.Close for
// all at once) and spawns runManagedUpdates. Deriving from m.baseCtx — not a
// request-scoped context — keeps a single caller's cancellation from tearing down
// the shared cache, and lets Close cancel every watch (including one born from a
// seed still in flight during Close: it starts on the already-cancelled base
// context and exits immediately). A watch that fails to open is non-fatal: the
// resolver keeps whatever the seed produced (fail-open) and simply gets no
// background refresh.
func (m *Manager) startManagedWatch(mr *managedResolver, name, tag string) {
	wctx, cancel := context.WithCancel(m.baseCtx)
	mr.cancel.Store(&cancel)

	ch, err := m.registry.Watch(wctx, name)
	if err != nil {
		cancel()
		m.logger.Log(wctx, log.LevelWarn, "managed resolver watch failed to start; serving seed only",
			log.String("service", name),
			log.Err(err))

		return
	}

	// SafeGo wraps the watch loop with panic recovery (KeepRunning): a panic in this
	// long-lived background goroutine is logged with its stack and terminates only
	// the goroutine, never the host process. A bare `go` would let any panic (e.g. a
	// nil deref inside a custom Registry) crash the whole service.
	obsruntime.SafeGo(m.logger, "libsd.managed-watch:"+name, obsruntime.KeepRunning, func() {
		m.runManagedUpdates(wctx, mr, ch, name, tag)
	})
}

// runManagedUpdates consumes catalog-change events and refreshes mr's cached
// Service. It stores the WHOLE Service (so every view derives from one cached
// value); on a registry failure it keeps the last-known-good rather than clearing
// the cache. It parallels runResolverUpdates (which projects a per-view address
// for the DynamicResolver) but is deliberately separate: the managed resolver is
// view-agnostic and caches the entire Service.
func (m *Manager) runManagedUpdates(ctx context.Context, mr *managedResolver, ch <-chan Event, name, tag string) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, open := <-ch:
			if !open {
				return
			}
		}

		svc, err := m.registry.Resolve(ctx, name, tag)
		if err != nil {
			// Keep the last-known-good value; a transient registry failure must not
			// blank a route consumers are still using.
			m.logger.Log(ctx, log.LevelWarn, "managed resolve update failed; keeping last value",
				log.String("service", name),
				log.Err(err))

			continue
		}

		mr.store(svc)
		m.logger.Log(ctx, log.LevelDebug, "managed resolve updated",
			log.String("service", name),
			log.String("addr", svc.Addr()))
	}
}
