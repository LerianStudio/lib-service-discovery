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

// seedDegradedMsg is the warning emitted when a resolver's seed fails but the
// resolver is still built and its watch still started (fail-open contract).
const seedDegradedMsg = "dynamic resolver seed degraded; starting watch anyway"

// blockingSeedRegistry blocks the FIRST Resolve (the seed) until its context is
// cancelled, then serves svc on every subsequent Resolve. It lets a test prove
// two things at once: the seed is bounded by SeedTimeout (GAP-2), and the
// long-lived watch — which drives the later Resolves — is NOT cancelled by the
// seed's short deadline (it still populates last-known-good).
type blockingSeedRegistry struct {
	mu      sync.Mutex
	calls   int
	svc     Service
	watchCh chan Event
}

func (r *blockingSeedRegistry) Register(_ context.Context, _ Service) error  { return nil }
func (r *blockingSeedRegistry) Deregister(_ context.Context, _ string) error { return nil }

func (r *blockingSeedRegistry) Resolve(ctx context.Context, _, _ string) (Service, error) {
	r.mu.Lock()
	r.calls++
	first := r.calls == 1
	svc := r.svc
	r.mu.Unlock()

	if first {
		// Hang the seed until its (short) deadline fires; honor ctx cancellation.
		<-ctx.Done()

		return Service{}, ctx.Err()
	}

	return svc, nil
}

func (r *blockingSeedRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	return r.watchCh, nil
}

// enabledManagerWithSeed builds an enabled Manager with a custom capture logger
// and seed timeout, so seed-degrade tests can assert the warning and keep the
// bounded-seed assertion fast.
func enabledManagerWithSeed(t *testing.T, reg Registry, cap *captureLogger, seedTimeout time.Duration) *Manager {
	t.Helper()

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		SeedTimeout:   seedTimeout,
		Logger:        cap,
	}, WithRegistry(reg))
	require.NoError(t, err)

	return m
}

// ── GAP-1: seed failure is non-fatal (fail-open) ────────────────────────────────

// A registry that errors on the seed and no fallback: the resolver must still be
// built (non-nil, no error), serve "" until the watch populates it, warn about the
// degrade, and then pick up the first successful catalog event.
func TestWatchResolve_SeedErrorNoFallbackIsNonFatal(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		err:     errors.New("consul down at boot"),
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}
	m := enabledManagerWithSeed(t, reg, cap, 3*time.Second)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)
	require.NotNil(t, dr)

	t.Cleanup(dr.Stop)

	// Fail-open: no seed value, but the resolver is live and watching.
	assert.Equal(t, "", dr.Address())
	assert.True(t, cap.has(seedDegradedMsg), "a failed seed must warn")

	// The watch recovers the address on the first successful catalog event.
	reg.setResolve(Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.1.5:3002"
	}, 2*time.Second, 10*time.Millisecond, "watch did not populate after a degraded seed")
}

// A registry that errors on the seed WITH a fallback: the resolver serves the
// fallback immediately, then upgrades to the resolved address on a catalog event.
func TestWatchResolve_SeedErrorWithFallbackServesFallbackThenPopulates(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		err:     errors.New("consul down at boot"),
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "fallback:9999")
	require.NoError(t, err)
	require.NotNil(t, dr)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "fallback:9999", dr.Address())

	reg.setResolve(Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.1.5:3002"
	}, 2*time.Second, 10*time.Millisecond, "watch did not upgrade off the fallback")
}

// WatchResolveService: seed failure with no fallback is non-fatal; the resolver
// serves empty addr/scheme until the watch populates both.
func TestWatchResolveService_SeedErrorNoFallbackIsNonFatal(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		err:     errors.New("consul down at boot"),
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}
	m := enabledManagerWithSeed(t, reg, cap, 3*time.Second)

	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", Service{})
	require.NoError(t, err)
	require.NotNil(t, dr)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "", dr.Address())
	assert.Equal(t, "", dr.Scheme())
	assert.True(t, cap.has(seedDegradedMsg), "a failed service seed must warn")

	reg.setResolve(Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "https"}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.1.5:3002" && dr.Scheme() == "https"
	}, 2*time.Second, 10*time.Millisecond, "service watch did not populate after a degraded seed")
}

// WatchResolveService: seed failure WITH a fallback serves the fallback's
// (view-independent, external) addr+scheme, then upgrades on a catalog event.
func TestWatchResolveService_SeedErrorWithFallbackServesFallbackThenPopulates(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		err:     errors.New("consul down at boot"),
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	fallback := Service{Address: "ledger.local", Port: 443, Scheme: "https"}

	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", fallback)
	require.NoError(t, err)
	require.NotNil(t, dr)

	t.Cleanup(dr.Stop)

	assert.Equal(t, "ledger.local:443", dr.Address())
	assert.Equal(t, "https", dr.Scheme())

	reg.setResolve(Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002, Scheme: "http"}}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.1.5:3002" && dr.Scheme() == "http"
	}, 2*time.Second, 10*time.Millisecond, "service watch did not upgrade off the fallback")
}

// ── GAP-2: seed is bounded by SeedTimeout, watch survives the seed deadline ──────

// A registry whose seed Resolve hangs must NOT block construction for the full
// response-header timeout: WatchResolve returns within ~SeedTimeout, degraded.
// The subsequent watch (driven by the resolver's own lifetime context, not the
// seed's) still resolves — proving the short seed deadline did not cancel it.
func TestWatchResolve_SeedTimeoutBoundsConstructionThenWatchPopulates(t *testing.T) {
	t.Parallel()

	reg := &blockingSeedRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.9.9", Port: 3002}},
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}
	m := enabledManagerWithSeed(t, reg, cap, 80*time.Millisecond)

	start := time.Now()
	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, dr)

	t.Cleanup(dr.Stop)

	// Bounded by SeedTimeout (~80ms), nowhere near the 10s response-header timeout.
	assert.Less(t, elapsed, time.Second, "seed must be bounded by SeedTimeout, not the response-header timeout")
	assert.Equal(t, "", dr.Address())
	assert.True(t, cap.has(seedDegradedMsg), "a timed-out seed must warn")

	// The watch's context outlived the seed deadline: a catalog event still resolves.
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.9.9:3002"
	}, 2*time.Second, 10*time.Millisecond, "watch was cancelled by the seed timeout")
}

// The bounded-seed guarantee applies to WatchResolveService too.
func TestWatchResolveService_SeedTimeoutBoundsConstruction(t *testing.T) {
	t.Parallel()

	reg := &blockingSeedRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.9.9", Port: 3002, Scheme: "https"}},
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}
	m := enabledManagerWithSeed(t, reg, cap, 80*time.Millisecond)

	start := time.Now()
	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", Service{})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, dr)

	t.Cleanup(dr.Stop)

	assert.Less(t, elapsed, time.Second, "service seed must be bounded by SeedTimeout")
	assert.Equal(t, "", dr.Address())
	assert.True(t, cap.has(seedDegradedMsg))

	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.9.9:3002" && dr.Scheme() == "https"
	}, 2*time.Second, 10*time.Millisecond, "service watch was cancelled by the seed timeout")
}

// ── Happy path preserved: a healthy seed still populates immediately ─────────────

func TestWatchResolve_HealthySeedPopulatesImmediately(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{External: &Endpoint{Address: "10.0.1.5", Port: 3002}},
		watchCh: make(chan Event, 1),
	}
	cap := &captureLogger{}
	m := enabledManagerWithSeed(t, reg, cap, 3*time.Second)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	// Seed synchronously populated Address() with no degrade warning.
	assert.Equal(t, "10.0.1.5:3002", dr.Address())
	assert.False(t, cap.has(seedDegradedMsg), "a healthy seed must not warn about degrade")
}

// ── Programmer error stays fatal: nil Manager ───────────────────────────────────

func TestWatchResolve_NilManagerIsFatal(t *testing.T) {
	t.Parallel()

	var m *Manager

	dr, err := m.WatchResolve(context.Background(), "svc", "fallback:1")
	assert.Nil(t, dr)
	assert.ErrorIs(t, err, ErrNilManager)
}

func TestWatchResolveService_NilManagerIsFatal(t *testing.T) {
	t.Parallel()

	var m *Manager

	dr, err := m.WatchResolveService(context.Background(), "svc", Service{Address: "x", Port: 1})
	assert.Nil(t, dr)
	assert.ErrorIs(t, err, ErrNilManager)
}
