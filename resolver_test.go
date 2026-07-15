//go:build unit

package libsd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// watchableRegistry is a Registry stub whose Watch channel and Resolve result
// are controllable from the test, so we can simulate catalog changes.
type watchableRegistry struct {
	mu       sync.Mutex
	svc      Service
	err      error
	watchCh  chan Event
	resolves int
}

func (r *watchableRegistry) Register(_ context.Context, _ Service) error  { return nil }
func (r *watchableRegistry) Deregister(_ context.Context, _ string) error { return nil }

func (r *watchableRegistry) Resolve(_ context.Context, _, _ string) (Service, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resolves++

	return r.svc, r.err
}

func (r *watchableRegistry) resolveCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.resolves
}

func (r *watchableRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	return r.watchCh, nil
}

func (r *watchableRegistry) setResolve(svc Service, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.svc = svc
	r.err = err
}

func TestWatchResolve_DisabledUsesFallback(t *testing.T) {
	t.Parallel()

	m := disabledManager(t)

	dr, err := m.WatchResolve(context.Background(), "svc", "fallback:9999")
	require.NoError(t, err)

	assert.Equal(t, "fallback:9999", dr.Address())

	dr.Stop() // no-op (no watch); must be safe
}

func TestWatchResolve_SeedsInitialAddress(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "10.0.1.5:3002", dr.Address())
}

func TestWatchResolve_UpdatesOnCatalogChange(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	require.Equal(t, "10.0.1.5:3002", dr.Address())

	// Provider re-registers under a new address; a catalog-change event fires.
	reg.setResolve(Service{External: &Endpoint{Address: "10.0.2.9", Port: 3002}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.2.9:3002"
	}, 2*time.Second, 10*time.Millisecond, "resolver did not pick up the new address")
}

func TestWatchResolveService_SeedsAddressAndScheme(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", Service{})
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "10.0.1.5:3002", dr.Address())
	assert.Equal(t, "https", dr.Scheme())
}

func TestWatchResolveService_UpdatesSchemeOnCatalogChange(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "http"}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", Service{})
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	require.Equal(t, "http", dr.Scheme())

	// Provider re-registers behind TLS; the resolver must follow the new scheme.
	reg.setResolve(Service{External: &Endpoint{Address: "10.0.2.9", Port: 3002, Scheme: "https"}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.2.9:3002" && dr.Scheme() == "https"
	}, 2*time.Second, 10*time.Millisecond, "resolver did not pick up the new address/scheme")
}

func TestWatchResolveService_DisabledUsesFallback(t *testing.T) {
	t.Parallel()

	m := disabledManager(t)

	dr, err := m.WatchResolveService(context.Background(), "svc",
		Service{Address: "ledger.local", Port: 443, Scheme: "https"})
	require.NoError(t, err)

	assert.Equal(t, "ledger.local:443", dr.Address())
	assert.Equal(t, "https", dr.Scheme())

	dr.Stop() // no-op (no watch); must be safe
}

// ── View selection (Epic 2.1) ──────────────────────────────────────────────────

func TestWatchResolve_WithViewSeedsInternal(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc: Service{
			External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"},
			Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
		},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "", WithView(Internal))
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "svc.ns.svc.cluster.local:9090", dr.Address())
}

func TestWatchResolveService_WithViewInternalTracksSchemeAndAddr(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc: Service{
			External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"},
			Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
		},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", Service{}, WithView(Internal))
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	// Seed reflects the internal endpoint (addr + scheme), not the external one.
	assert.Equal(t, "svc.ns.svc.cluster.local:9090", dr.Address())
	assert.Equal(t, "http", dr.Scheme())

	// Provider re-registers; the internal endpoint moves and switches to TLS.
	reg.setResolve(Service{
		Address: "10.0.2.9", Port: 3002, Scheme: "https",
		Internal: &Endpoint{Address: "svc2.ns.svc.cluster.local", Port: 9091, Scheme: "https"},
	}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "svc2.ns.svc.cluster.local:9091" && dr.Scheme() == "https"
	}, 2*time.Second, 10*time.Millisecond, "resolver did not track the new internal endpoint")
}

func TestWatchResolve_WithViewInternalFallsBackToExternalWhenInternalNil(t *testing.T) {
	t.Parallel()

	// Provider advertised only the external endpoint (Internal nil).
	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "", WithView(Internal))
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	// EndpointFor(Internal) degrades to the external endpoint.
	assert.Equal(t, "10.0.1.5:3002", dr.Address())
}

func TestWatchResolve_DefaultViewIsExternal(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc: Service{
			External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"},
			Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
		},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	// No WithView → zero-value view "" resolves as External.
	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "10.0.1.5:3002", dr.Address())
}

func TestWatchResolve_WithViewFallbackIsViewIndependent(t *testing.T) {
	t.Parallel()

	// Registry errors, so the seed must use the fallback host:port as-is.
	reg := &watchableRegistry{
		err:     errors.New("consul down"),
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "fallback:9999", WithView(Internal))
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	// Fallback is a resolved host:port; WithView must not rewrite it.
	assert.Equal(t, "fallback:9999", dr.Address())
}

func TestWatchResolveService_WithViewInternalFallbackStaysExternal(t *testing.T) {
	t.Parallel()

	// Discovery enabled, but the INITIAL resolve fails, so the seed must use the
	// fallback. The fallback carries an Internal endpoint and the resolver is
	// created WithView(Internal). Per the "fallback is view-independent" contract,
	// the seed must store the fallback's EXTERNAL endpoint — never
	// EndpointFor(Internal) applied to the fallback Service. A subsequent catalog
	// event that also fails must keep the external endpoint (no silent
	// internal→external / http→https flip between seed and runtime).
	reg := &watchableRegistry{
		err:     errors.New("consul down"),
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	fallback := Service{
		Address: "ext.example.com", Port: 8080, Scheme: "https",
		Internal: &Endpoint{Address: "internal.svc.cluster.local", Port: 9090, Scheme: "http"},
	}

	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", fallback, WithView(Internal))
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	// Seed: the external fallback endpoint, not the internal one.
	require.Equal(t, "ext.example.com:8080", dr.Address())
	require.Equal(t, "https", dr.Scheme())

	// A catalog event fires while the registry is still failing; the runtime
	// fallback branch must land on the same external endpoint as the seed.
	reg.watchCh <- Event{Type: EventDeregistered}

	// Confirm the update loop actually CONSUMED the event and re-resolved before
	// asserting the retained state, so the assertion is deterministic rather than
	// passing because the event was never processed. resolveCalls goes to 2 = the
	// failed seed (1) + the failed watch update (1).
	require.Eventually(t, func() bool {
		return reg.resolveCalls() >= 2
	}, time.Second, 10*time.Millisecond, "update loop must consume the catalog event and re-resolve")

	require.Never(t, func() bool {
		return dr.Address() != "ext.example.com:8080" || dr.Scheme() != "https"
	}, 300*time.Millisecond, 20*time.Millisecond,
		"fallback must stay view-independent (external) across seed and update")
}

// ── PreferView default (Epic 2.2) ───────────────────────────────────────────────

func TestWatchResolve_PreferViewDefaultAppliesWhenNoWithView(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc: Service{
			External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"},
			Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
		},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)
	m.preferView = Internal // config default; no WithView passed below

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	// The config default (Internal) supplies dr.view when WithView is absent.
	assert.Equal(t, "svc.ns.svc.cluster.local:9090", dr.Address())
}

func TestWatchResolve_WithViewOverridesPreferView(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc: Service{
			External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"},
			Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
		},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)
	m.preferView = Internal // config default …

	// … but an explicit WithView(External) must win over the config default.
	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "", WithView(External))
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "10.0.1.5:3002", dr.Address())
}

func TestDynamicResolver_NilReceiverIsSafe(t *testing.T) {
	t.Parallel()

	var dr *DynamicResolver

	assert.Equal(t, "", dr.Address())
	assert.Equal(t, "", dr.Scheme())
	dr.Stop() // must not panic on a nil receiver
}

// TestDynamicResolver_StoreWritesOneSnapshot proves #18: address and scheme live
// in ONE immutable snapshot written atomically, so a reader can never observe a
// torn pair (new address + stale scheme). It is white-box on purpose — it reads
// dr.snapshot, so reverting to two separate atomic fields would fail to compile.
func TestDynamicResolver_StoreWritesOneSnapshot(t *testing.T) {
	t.Parallel()

	dr := &DynamicResolver{}
	dr.store("10.0.0.2:8443", "https")

	snap := dr.snapshot.Load()
	require.NotNil(t, snap, "store must publish a snapshot")
	assert.Equal(t, "10.0.0.2:8443", snap.address)
	assert.Equal(t, "https", snap.scheme)

	// The public accessors read from that same single snapshot.
	assert.Equal(t, "10.0.0.2:8443", dr.Address())
	assert.Equal(t, "https", dr.Scheme())

	// A subsequent store replaces the whole snapshot atomically (no field-by-field
	// mutation that a concurrent reader could catch half-applied).
	dr.store("10.0.0.9:80", "http")
	assert.Equal(t, "10.0.0.9:80", dr.Address())
	assert.Equal(t, "http", dr.Scheme())
}

// TestDynamicResolver_ConcurrentStoreAndReadRaceClean hammers store while readers
// call Address()/Scheme(), so `-race` proves the single atomic.Pointer snapshot is
// race-free. Each individual read must land on a fully-consistent snapshot.
func TestDynamicResolver_ConcurrentStoreAndReadRaceClean(t *testing.T) {
	t.Parallel()

	const (
		addrA, schemeA = "10.0.0.1:3002", "http"
		addrB, schemeB = "10.0.0.2:8443", "https"
	)

	dr := &DynamicResolver{}
	dr.store(addrA, schemeA)

	done := make(chan struct{})

	go func() {
		defer close(done)

		for i := 0; i < 50000; i++ {
			if i%2 == 0 {
				dr.store(addrA, schemeA)
			} else {
				dr.store(addrB, schemeB)
			}
		}
	}()

	for i := 0; i < 50000; i++ {
		// A single Load must never expose a half-written snapshot.
		if snap := dr.snapshot.Load(); snap != nil {
			consistent := (snap.address == addrA && snap.scheme == schemeA) ||
				(snap.address == addrB && snap.scheme == schemeB)
			require.True(t, consistent, "torn snapshot: address=%q scheme=%q", snap.address, snap.scheme)
		}
	}

	<-done
}

func TestWatchResolve_UpdateFallsBackThenKeepsLast(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "fallback:9999")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	require.Equal(t, "10.0.1.5:3002", dr.Address())

	// Registry starts failing; with a fallback set, the resolver swaps to it.
	reg.setResolve(Service{}, errors.New("consul down"))
	reg.watchCh <- Event{Type: EventDeregistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "fallback:9999"
	}, 2*time.Second, 10*time.Millisecond, "resolver did not fall back on registry error")
}

func TestWatchResolve_UpdateKeepsLastWhenNoFallback(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	require.Equal(t, "10.0.1.5:3002", dr.Address())

	// Registry fails and there is no fallback: the last known good value is kept.
	reg.setResolve(Service{}, errors.New("consul down"))
	reg.watchCh <- Event{Type: EventDeregistered}

	// Give the goroutine time to process the event, then assert nothing changed.
	require.Never(t, func() bool {
		return dr.Address() != "10.0.1.5:3002"
	}, 300*time.Millisecond, 20*time.Millisecond, "resolver must keep last value when no fallback")
}

// ── View unavailability & degrade (Epic 3.6) ────────────────────────────────────

// Seed with an External view against an internal-only provider, no fallback: the
// seed's EndpointFor(External) surfaces ErrEndpointViewUnavailable, but the seed
// is fail-open — WatchResolve must NOT fail. It builds the resolver, degrades to
// an empty address, warns, and starts the watch so a later catalog change can
// recover the view.
func TestWatchResolve_SeedExternalViewInternalOnlyIsNonFatal(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}},
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}
	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        cap,
	}, WithRegistry(reg))
	require.NoError(t, err)

	dr, err := m.WatchResolve(context.Background(), "svc", "", WithView(External))
	require.NoError(t, err)
	require.NotNil(t, dr)
	t.Cleanup(dr.Stop)

	assert.Equal(t, "", dr.Address())
	assert.True(t, cap.has("dynamic resolver seed degraded; starting watch anyway"),
		"an ErrEndpointViewUnavailable seed must degrade, not fail")
}

// Same fail-open seed contract for WatchResolveService (exercises seedServiceEndpoint).
func TestWatchResolveService_SeedExternalViewInternalOnlyIsNonFatal(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}},
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}
	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        cap,
	}, WithRegistry(reg))
	require.NoError(t, err)

	dr, err := m.WatchResolveService(context.Background(), "svc", Service{}, WithView(External))
	require.NoError(t, err)
	require.NotNil(t, dr)
	t.Cleanup(dr.Stop)

	assert.Equal(t, "", dr.Address())
	assert.Equal(t, "", dr.Scheme())
	assert.True(t, cap.has("dynamic resolver seed degraded; starting watch anyway"),
		"an ErrEndpointViewUnavailable service seed must degrade, not fail")
}

// Seed with an Internal view against an external-only provider degrades to the
// external endpoint AND emits the degrade warning (via seedServiceEndpoint).
func TestWatchResolveService_SeedInternalDegradeWarns(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"}},
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        cap,
	}, WithRegistry(reg))
	require.NoError(t, err)

	dr, err := m.WatchResolveService(context.Background(), "svc", Service{}, WithView(Internal))
	require.NoError(t, err)
	t.Cleanup(dr.Stop)

	assert.Equal(t, "10.0.1.5:3002", dr.Address())
	assert.True(t, cap.has("internal view degraded to external"),
		"a real internal->external seed degrade must warn")
}

// On a catalog update the provider becomes internal-only, so the resolver's
// External view is unavailable: the last known good value is kept (never an empty
// address) and a warning is emitted.
func TestWatchResolve_UpdateViewUnavailableKeepsLast(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"}},
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        cap,
	}, WithRegistry(reg))
	require.NoError(t, err)

	// Default view is External; no fallback so a failed update keeps the last value.
	dr, err := m.WatchResolve(context.Background(), "svc", "")
	require.NoError(t, err)
	t.Cleanup(dr.Stop)

	require.Equal(t, "10.0.1.5:3002", dr.Address())

	// Provider re-registers as internal-only: External view is now unavailable.
	reg.setResolve(Service{Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	// The cached address must never change to an empty/internal value.
	require.Never(t, func() bool {
		return dr.Address() != "10.0.1.5:3002"
	}, 300*time.Millisecond, 20*time.Millisecond, "view-unavailable update must keep the last value")

	assert.Eventually(t, func() bool {
		return cap.has("dynamic resolve view unavailable; keeping last value")
	}, time.Second, 10*time.Millisecond, "view-unavailable update must warn")
}

func TestWatchResolve_StopHaltsUpdates(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	dr.Stop()
	time.Sleep(100 * time.Millisecond) // let the goroutine observe ctx.Done and exit

	// With the watcher stopped, an event enqueues on the buffered channel but is
	// never consumed, so the cached address must stay unchanged.
	reg.setResolve(Service{External: &Endpoint{Address: "10.0.2.9", Port: 3002}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, "10.0.1.5:3002", dr.Address())
}
