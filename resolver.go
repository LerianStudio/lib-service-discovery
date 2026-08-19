package libsd

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/LerianStudio/lib-observability/v2/log"
	obsruntime "github.com/LerianStudio/lib-observability/v2/runtime"
)

// DynamicResolver keeps a service's address fresh. It seeds an initial value via
// Resolve, then watches the catalog and updates the cached address whenever the
// service changes. Callers read Address() before each outbound call and always
// get the latest healthy endpoint, without re-querying Consul on the hot path.
//
// This fixes staleness in the resolve-once-and-cache pattern: when a provider
// re-registers under a new address, consumers observe the change without
// restarting. The underlying Watch is an outbound long-poll, so it works for
// agentless/remote workloads behind NAT (the central-Consul model).
type DynamicResolver struct {
	fallback       string
	fallbackScheme string
	// view selects which advertised endpoint to track (External or Internal). The
	// zero value "" is treated as External at the point of use (EndpointFor maps
	// unknown → external).
	view EndpointView
	// snapshot holds the current {address, scheme} as ONE immutable value, so a
	// reader can never observe a torn pair (a new address with a stale scheme).
	// Written atomically by the seed and every update; read by Address()/Scheme().
	snapshot atomic.Pointer[resolverSnapshot]
	cancel   context.CancelFunc
}

// resolverSnapshot is the immutable address+scheme pair a DynamicResolver stores
// atomically, so Address() and Scheme() always read a consistent view.
type resolverSnapshot struct {
	address string
	scheme  string
}

// ResolverOption configures a DynamicResolver at construction time.
type ResolverOption func(*DynamicResolver)

// WithView selects which advertised endpoint the resolver tracks: External
// (ingress, the default) or Internal (in-cluster K8s DNS). A resolver created
// without WithView keeps the zero-value view "", which resolves as External.
func WithView(view EndpointView) ResolverOption {
	return func(dr *DynamicResolver) {
		if dr == nil {
			return
		}

		dr.view = view
	}
}

// Address returns the most recently resolved address for the service.
// Before the first update it returns the seed value (initial resolve or fallback).
func (d *DynamicResolver) Address() string {
	if d == nil {
		return ""
	}

	if s := d.snapshot.Load(); s != nil {
		return s.address
	}

	return d.fallback
}

// Scheme returns the most recently resolved URL scheme for the service
// (e.g. "https"), or "" when unknown. Populated only when the resolver was
// created via WatchResolveService; WatchResolve leaves it empty.
func (d *DynamicResolver) Scheme() string {
	if d == nil {
		return ""
	}

	if s := d.snapshot.Load(); s != nil {
		return s.scheme
	}

	return d.fallbackScheme
}

// store atomically records the current address+scheme as one immutable snapshot,
// so Address() and Scheme() never observe a torn pair.
func (d *DynamicResolver) store(address, scheme string) {
	d.snapshot.Store(&resolverSnapshot{address: address, scheme: scheme})
}

// Stop ends the background watch and releases its resources.
// Safe to call multiple times and on a nil receiver.
func (d *DynamicResolver) Stop() {
	if d == nil || d.cancel == nil {
		return
	}

	d.cancel()
}

// WatchResolve returns a DynamicResolver that keeps name's host:port up to date.
// The resolved scheme is not tracked (Scheme() returns ""); use
// WatchResolveService when the consumer needs the discovered scheme too.
//
//   - Discovery disabled: returns a resolver pinned to fallback (no watch);
//     errors with ErrDiscoveryDisabledNoFallback when fallback is empty.
//   - Discovery enabled: seeds via Resolve, then a background goroutine re-resolves
//     name on every catalog change, updating the cached address (or using fallback).
//
// The returned resolver must be Stop()ped to release the watch goroutine.
func (m *Manager) WatchResolve(ctx context.Context, name, fallback string, opts ...ResolverOption) (*DynamicResolver, error) {
	if m == nil {
		return nil, ErrNilManager
	}

	dr := &DynamicResolver{fallback: fallback}

	for _, opt := range opts {
		if opt != nil {
			opt(dr)
		}
	}

	// Precedence: explicit WithView > config default (PreferView) > External. When
	// no WithView set dr.view, fall back to the Manager's configured default.
	if dr.view == "" {
		dr.view = m.preferView
	}

	// Disabled mode is unchanged: a static fallback with no watch. An empty
	// fallback here is a caller error (ErrDiscoveryDisabledNoFallback), not a
	// discovery-server failure, so it stays fatal. The seed uses the private direct
	// helper (not the public ResolveEndpoint) so this DynamicResolver never also
	// spins up a managed resolver + watcher for the same name.
	if !m.config.Enabled {
		addr, err := m.resolveEndpointDirect(ctx, name, dr.view, fallback)
		if err != nil {
			return nil, err
		}

		dr.store(addr, "")

		return m.startResolverWatch(ctx, dr, name)
	}

	// Enabled mode is fail-open. Seed the initial value under a short, SEPARATE
	// deadline derived from ctx so a slow/dead discovery server cannot hang boot
	// for the full response-header timeout. A seed failure is never fatal: build
	// the resolver, seed with the fallback (or empty), and start the watch anyway —
	// runResolverUpdates populates last-known-good on its first success. The seed
	// goes through the private direct helper so it never creates a phantom managed
	// watcher for the same name.
	seedCtx, cancel := context.WithTimeout(ctx, m.seedTimeout)
	addr, err := m.resolveEndpointDirect(seedCtx, name, dr.view, fallback)

	cancel() // release the seed deadline immediately; the watch uses ctx, not seedCtx.

	if err != nil {
		dr.store(fallback, "")
		m.logger.Log(ctx, log.LevelWarn, "dynamic resolver seed degraded; starting watch anyway",
			log.String("service", name),
			log.String("view", string(dr.view)),
			log.String("fallback", fallback),
			log.Err(err))
	} else {
		dr.store(addr, "")
	}

	return m.startResolverWatch(ctx, dr, name)
}

// WatchResolveService is like WatchResolve but tracks the full Service, so the
// resolved scheme is available via Scheme() in addition to the host:port from
// Address(). Use it when the consumer must follow a scheme advertised by the
// provider (e.g. an HTTPS endpoint discovered without changing the local URL).
//
// fallback seeds both Address() and Scheme() when discovery is disabled or
// Consul has no healthy instance; an empty fallback.Address means "no fallback".
//
// The returned resolver must be Stop()ped to release the watch goroutine.
func (m *Manager) WatchResolveService(ctx context.Context, name string, fallback Service, opts ...ResolverOption) (*DynamicResolver, error) {
	if m == nil {
		return nil, ErrNilManager
	}

	// An empty fallback.Address means "no fallback": keep dr.fallback "" rather than
	// the ":0" that Addr() would synthesize, so the "no fallback" contract holds in
	// both the seed-degrade branch and the runtime update loop (which gates on
	// dr.fallback != "").
	dr := &DynamicResolver{}
	if fallback.Address != "" {
		dr.fallback = fallback.Addr()
		dr.fallbackScheme = fallback.Scheme
	}

	for _, opt := range opts {
		if opt != nil {
			opt(dr)
		}
	}

	// Precedence: explicit WithView > config default (PreferView) > External. When
	// no WithView set dr.view, fall back to the Manager's configured default.
	if dr.view == "" {
		dr.view = m.preferView
	}

	// Disabled mode is unchanged: a static fallback with no watch. An empty
	// fallback here is a caller error (ErrDiscoveryDisabledNoFallback), not a
	// discovery-server failure, so it stays fatal.
	if !m.config.Enabled {
		addr, scheme, err := m.seedServiceEndpoint(ctx, name, dr, fallback)
		if err != nil {
			return nil, err
		}

		dr.store(addr, scheme)

		return m.startResolverWatch(ctx, dr, name)
	}

	// Enabled mode is fail-open. Seed under a short, SEPARATE deadline derived from
	// ctx so a slow/dead discovery server cannot hang boot for the full
	// response-header timeout. A genuine registry hit is mapped through the selected
	// view; every fallback path stays view-independent (the external endpoint),
	// mirroring the runtime update loop. A seed failure is never fatal: seed with
	// the fallback's (view-independent) addr+scheme and start the watch anyway.
	seedCtx, cancel := context.WithTimeout(ctx, m.seedTimeout)
	addr, scheme, err := m.seedServiceEndpoint(seedCtx, name, dr, fallback)

	cancel() // release the seed deadline immediately; the watch uses ctx, not seedCtx.

	if err != nil {
		dr.store(dr.fallback, dr.fallbackScheme)
		m.logger.Log(ctx, log.LevelWarn, "dynamic resolver seed degraded; starting watch anyway",
			log.String("service", name),
			log.String("view", string(dr.view)),
			log.String("fallback", dr.fallback),
			log.Err(err))
	} else {
		dr.store(addr, scheme)
	}

	return m.startResolverWatch(ctx, dr, name)
}

// resolveEndpointDirect is a one-shot endpoint resolution straight against the
// registry, bypassing the managed watch-and-cache layer. It is the seed helper for
// WatchResolve, whose DynamicResolver owns its OWN watch — routing the seed through
// the public ResolveEndpoint would additionally create a managed resolver + watcher
// for the same name (a phantom watch). Its behavior mirrors the one-shot endpoint
// resolution exactly: disabled → fallback or ErrDiscoveryDisabledNoFallback; a hit
// mapped through view (External-view-unavailable → fallback or the view error;
// Internal degrade → external + warn); a registry failure → fallback or the error.
func (m *Manager) resolveEndpointDirect(ctx context.Context, name string, view EndpointView, fallback string) (string, error) {
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

	svc, err := m.registry.Resolve(ctx, name, m.workloadTag())
	if err == nil {
		ep, epErr := svc.EndpointFor(view)
		if epErr != nil {
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

		if view == Internal && svc.Internal == nil {
			m.logger.Log(ctx, log.LevelWarn, "internal view degraded to external",
				log.String("service", name),
				log.String("addr", ep.Addr()),
				log.String("view", string(view)))
		}

		return ep.Addr(), nil
	}

	if fallback != "" {
		m.logger.Log(ctx, log.LevelWarn, "service resolved",
			log.String("service", name),
			log.String("addr", fallback),
			log.String("source", "fallback"),
			log.String("reason", "consul resolve failed"),
			log.Err(err))

		return fallback, nil
	}

	return "", err
}

// seedServiceEndpoint computes the initial addr+scheme for WatchResolveService.
// It mirrors ResolveService's branch structure, with one critical difference that
// enforces the resolver contract: EndpointFor(dr.view) is applied ONLY to a
// genuine registry hit. Every fallback path — discovery disabled, or a resolve
// failure with a usable fallback — returns the fallback's EXTERNAL endpoint
// (dr.fallback/dr.fallbackScheme), keeping the fallback view-independent and
// matching the runtime update loop's fallback branch. Applying EndpointFor to a
// fallback Service would leak a view-dependent (e.g. Internal) endpoint into a
// value the contract guarantees is view-independent.
func (m *Manager) seedServiceEndpoint(ctx context.Context, name string, dr *DynamicResolver, fallback Service) (addr, scheme string, err error) {
	if !m.config.Enabled {
		if fallback.Address == "" {
			return "", "", fmt.Errorf("%w: %q", ErrDiscoveryDisabledNoFallback, name)
		}

		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", dr.fallback),
			log.String("source", "fallback"),
			log.String("reason", "discovery disabled"))

		return dr.fallback, dr.fallbackScheme, nil
	}

	tag := m.workloadTag()

	svc, resolveErr := m.registry.Resolve(ctx, name, tag)
	if resolveErr == nil {
		// Genuine hit: map through the selected view.
		ep, epErr := svc.EndpointFor(dr.view)
		if epErr == nil {
			// Internal view satisfied by degrading to external: warn.
			if dr.view == Internal && svc.Internal == nil {
				m.logger.Log(ctx, log.LevelWarn, "internal view degraded to external",
					log.String("service", name),
					log.String("addr", ep.Addr()),
					log.String("view", string(dr.view)))
			}

			m.logger.Log(ctx, log.LevelInfo, "service resolved",
				log.String("service", name),
				log.String("addr", ep.Addr()),
				log.String("source", "consul"),
				log.String("scheme", ep.Scheme),
				log.String("workload", m.workload),
				log.String("view", string(dr.view)))

			return ep.Addr(), ep.Scheme, nil
		}

		// The requested view is unavailable (External view against an internal-only
		// provider): treat as a miss and take the fallback branch below.
		resolveErr = epErr
	}

	if fallback.Address != "" {
		// Fallback is the already-resolved external endpoint — view-independent,
		// never re-mapped (identical to the runtime update loop's fallback branch).
		m.logger.Log(ctx, log.LevelWarn, "service resolved",
			log.String("service", name),
			log.String("addr", dr.fallback),
			log.String("source", "fallback"),
			log.String("reason", "consul resolve failed"),
			log.Err(resolveErr))

		return dr.fallback, dr.fallbackScheme, nil
	}

	return "", "", resolveErr
}

// startResolverWatch wires the background watch for an already-seeded resolver.
// In disabled mode it is a no-op (the seed is static); otherwise it opens the
// catalog Watch and spawns the update goroutine.
func (m *Manager) startResolverWatch(ctx context.Context, dr *DynamicResolver, name string) (*DynamicResolver, error) {
	// Disabled mode: static address, nothing to watch.
	if !m.config.Enabled {
		return dr, nil
	}

	wctx, cancel := context.WithCancel(ctx)
	dr.cancel = cancel

	ch, err := m.registry.Watch(wctx, name)
	if err != nil {
		cancel()

		return nil, err
	}

	tag := m.workloadTag()

	// SafeGo wraps the update loop with panic recovery (KeepRunning): a panic in a
	// single catalog-update cycle must not tear down the process — parity with the
	// managed watch (runManagedUpdates) and RegisterAsync goroutines. Lifecycle is
	// unchanged: wctx (cancelled by dr.cancel/Close) still stops the loop.
	obsruntime.SafeGo(m.logger, "libsd.resolver-watch:"+name, obsruntime.KeepRunning, func() {
		m.runResolverUpdates(wctx, dr, ch, name, tag)
	})

	return dr, nil
}

// runResolverUpdates consumes catalog-change events and refreshes dr's cached
// address and scheme. Each event triggers a re-resolve with the correct
// passing/workload filter; on failure it uses fallback, or keeps the last
// known good value.
func (m *Manager) runResolverUpdates(ctx context.Context, dr *DynamicResolver, ch <-chan Event, name, tag string) {
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
			// Registry failure: fall back when provided, else keep the last value.
			switch {
			case dr.fallback != "":
				// Fallback is an already-resolved host:port — view-independent, not re-mapped.
				dr.store(dr.fallback, dr.fallbackScheme)
				m.logger.Log(ctx, log.LevelWarn, "dynamic resolve fell back",
					log.String("service", name),
					log.String("fallback", dr.fallback),
					log.Err(err))
			default:
				m.logger.Log(ctx, log.LevelWarn, "dynamic resolve update failed; keeping last value",
					log.String("service", name),
					log.Err(err))
			}

			continue
		}

		ep, epErr := svc.EndpointFor(dr.view)
		if epErr != nil {
			// The requested view is unavailable (External view against an
			// internal-only provider). Keep the last known good value rather than
			// storing an empty address; never crash.
			m.logger.Log(ctx, log.LevelWarn, "dynamic resolve view unavailable; keeping last value",
				log.String("service", name),
				log.String("view", string(dr.view)),
				log.Err(epErr))

			continue
		}

		// Internal view satisfied by degrading to external: warn.
		if dr.view == Internal && svc.Internal == nil {
			m.logger.Log(ctx, log.LevelWarn, "internal view degraded to external",
				log.String("service", name),
				log.String("addr", ep.Addr()),
				log.String("view", string(dr.view)))
		}

		dr.store(ep.Addr(), ep.Scheme)
		m.logger.Log(ctx, log.LevelDebug, "dynamic resolve updated",
			log.String("service", name),
			log.String("addr", ep.Addr()),
			log.String("scheme", ep.Scheme),
			log.String("view", string(dr.view)))
	}
}
