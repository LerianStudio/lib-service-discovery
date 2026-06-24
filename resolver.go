package libsd

import (
	"context"
	"sync/atomic"

	"github.com/LerianStudio/lib-observability/log"
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
	current        atomic.Value // string — host:port
	currentScheme  atomic.Value // string — URL scheme ("https"/"http"/"")
	cancel         context.CancelFunc
}

// Address returns the most recently resolved address for the service.
// Before the first update it returns the seed value (initial resolve or fallback).
func (d *DynamicResolver) Address() string {
	if d == nil {
		return ""
	}

	if v, ok := d.current.Load().(string); ok {
		return v
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

	if v, ok := d.currentScheme.Load().(string); ok {
		return v
	}

	return d.fallbackScheme
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
func (m *Manager) WatchResolve(ctx context.Context, name, fallback string) (*DynamicResolver, error) {
	if m == nil {
		return nil, ErrNilManager
	}

	dr := &DynamicResolver{fallback: fallback}

	// Seed the initial value (also surfaces config errors early).
	addr, err := m.Resolve(ctx, name, fallback)
	if err != nil {
		return nil, err
	}

	dr.current.Store(addr)

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
func (m *Manager) WatchResolveService(ctx context.Context, name string, fallback Service) (*DynamicResolver, error) {
	if m == nil {
		return nil, ErrNilManager
	}

	dr := &DynamicResolver{fallback: fallback.Addr(), fallbackScheme: fallback.Scheme}

	// Seed the initial value (also surfaces config errors early).
	svc, err := m.ResolveService(ctx, name, fallback)
	if err != nil {
		return nil, err
	}

	dr.current.Store(svc.Addr())
	dr.currentScheme.Store(svc.Scheme)

	return m.startResolverWatch(ctx, dr, name)
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

	tag := ""
	if m.workload != "" {
		tag = "workload=" + m.workload
	}

	go m.runResolverUpdates(wctx, dr, ch, name, tag)

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

		switch {
		case err == nil:
			dr.current.Store(svc.Addr())
			dr.currentScheme.Store(svc.Scheme)
			m.logger.Log(ctx, log.LevelDebug, "dynamic resolve updated",
				log.String("service", name),
				log.String("addr", svc.Addr()),
				log.String("scheme", svc.Scheme))
		case dr.fallback != "":
			dr.current.Store(dr.fallback)
			dr.currentScheme.Store(dr.fallbackScheme)
			m.logger.Log(ctx, log.LevelWarn, "dynamic resolve fell back",
				log.String("service", name),
				log.String("fallback", dr.fallback),
				log.Err(err))
		default:
			m.logger.Log(ctx, log.LevelWarn, "dynamic resolve update failed; keeping last value",
				log.String("service", name),
				log.Err(err))
		}
	}
}
