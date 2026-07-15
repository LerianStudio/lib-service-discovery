//go:build unit

package libsd

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingRegistry counts Resolve/Watch calls so a test can prove the managed
// watch-and-cache layer seeds once and then serves reads from cache without
// re-querying the registry. Its Watch returns watchCh when set (a live watch a
// test can drive) or a closed channel (the watcher exits immediately).
type countingRegistry struct {
	mu       sync.Mutex
	resolves int
	watches  int
	svc      Service
	err      error
	watchCh  chan Event
}

func (r *countingRegistry) Register(context.Context, Service) error  { return nil }
func (r *countingRegistry) Deregister(context.Context, string) error { return nil }

func (r *countingRegistry) Resolve(_ context.Context, _, _ string) (Service, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resolves++

	return r.svc, r.err
}

func (r *countingRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.watches++

	if r.watchCh != nil {
		return r.watchCh, nil
	}

	ch := make(chan Event)
	close(ch)

	return ch, nil
}

func (r *countingRegistry) set(svc Service, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.svc = svc
	r.err = err
}

func (r *countingRegistry) resolveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.resolves
}

func (r *countingRegistry) watchCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.watches
}

// blockingCloseRegistry lets a test hold the lazy seed BLOCKED in flight (inside
// Resolve) while Close runs, so it can prove that a watch born after Close never
// leaks. Resolve signals entry on entered, then parks until release is closed or
// ctx is cancelled. Watch returns a never-closed channel so a watcher that (wrongly)
// started on a live context would park forever and show up as a leaked goroutine.
type blockingCloseRegistry struct {
	entered chan struct{}
	release chan struct{}
	watchCh chan Event
	watches atomic.Int32
}

func newBlockingCloseRegistry() *blockingCloseRegistry {
	return &blockingCloseRegistry{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		watchCh: make(chan Event),
	}
}

func (r *blockingCloseRegistry) Register(context.Context, Service) error  { return nil }
func (r *blockingCloseRegistry) Deregister(context.Context, string) error { return nil }

func (r *blockingCloseRegistry) Resolve(ctx context.Context, _, _ string) (Service, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}

	select {
	case <-r.release:
		return managedService("10.0.0.1", 9000, "http"), nil
	case <-ctx.Done():
		return Service{}, ctx.Err()
	}
}

func (r *blockingCloseRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	r.watches.Add(1)

	return r.watchCh, nil
}

// TestManagedResolver_CloseDuringInFlightSeedDoesNotLeak is the leak regression:
// Close fires while a first-resolve seed is blocked in flight. The watch is born
// only after the seed returns — after Close — yet because managed watches derive
// from the Manager's base context (cancelled by Close) it must exit immediately.
//
// The leak assertion is NumGoroutine() <= baseline settled via require.Eventually
// (not an exact == snapshot). A leaked watcher is a PERSISTENT +1 over baseline:
// it never drops back, so <=baseline can never become true and Eventually fails —
// the leak is caught. Goroutines still draining from earlier tests are TRANSIENT
// (count temporarily above baseline): they finish inside the window and the
// condition converges, so a cold `-race` run does not flake. This is a real
// bound, not a tolerance: it does not mask a persistent +1.
//
// Against the pre-fix code (watch derived from context.Background(), never
// cancelled by Close) the late-born watcher parks forever and this fails.
//
// PROJECT_RULES keeps dependencies minimal, so this uses runtime.NumGoroutine()
// rather than goleak.
//
// Intentionally NOT parallel: runtime.NumGoroutine() is only stable when no other
// test runs concurrently.
func TestManagedResolver_CloseDuringInFlightSeedDoesNotLeak(t *testing.T) {
	var prev, baseline int

	require.Eventually(t, func() bool {
		prev = baseline
		baseline = runtime.NumGoroutine()

		return baseline > 0 && baseline == prev
	}, time.Second, 20*time.Millisecond, "goroutine count did not stabilize for baseline")

	reg := newBlockingCloseRegistry()

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        log.NewNop(),
	}, WithRegistry(reg))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		_, _ = m.Resolve(context.Background(), "svc-a", "fallback:1")
	}()

	// Wait until the seed is genuinely parked in flight before closing.
	select {
	case <-reg.entered:
	case <-time.After(time.Second):
		t.Fatal("seed did not enter Resolve")
	}

	// Close during the in-flight seed: sets closed, cancels the base context,
	// drains the map. The watch is not yet started (seed still parked).
	require.NoError(t, m.Close())

	// Release the seed. startManagedWatch now runs, but on the cancelled base
	// context, so runManagedUpdates must return immediately — no leak.
	close(reg.release)

	wg.Wait()

	// <=baseline (not ==): a leaked watcher is a persistent +1 that never satisfies
	// this, so Eventually fails and catches the leak; transient drainers converge.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline
	}, 5*time.Second, 20*time.Millisecond,
		"a watch born after Close leaked (baseline=%d, now=%d)", baseline, runtime.NumGoroutine())
}

// TestManagedResolver_ResolveAfterCloseDoesNotLeakWatcher proves a Resolve issued
// AFTER Close never resurrects a background watch: the Manager is closed, so
// managedResolverFor returns a degraded, un-seeded resolver and starts no watcher.
// The request degrades to the fallback and the goroutine count stays at baseline.
//
// Against the pre-fix code (no closed flag) the post-Close resolve created a fresh
// resolver and started a watch on context.Background(), leaking a goroutine.
//
// Intentionally NOT parallel: runtime.NumGoroutine() is only stable when no other
// test runs concurrently.
func TestManagedResolver_ResolveAfterCloseDoesNotLeakWatcher(t *testing.T) {
	var prev, baseline int

	require.Eventually(t, func() bool {
		prev = baseline
		baseline = runtime.NumGoroutine()

		return baseline > 0 && baseline == prev
	}, time.Second, 20*time.Millisecond, "goroutine count did not stabilize for baseline")

	reg := &countingRegistry{
		svc:     managedService("10.0.0.1", 9000, "https"),
		watchCh: make(chan Event),
	}

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        log.NewNop(),
	}, WithRegistry(reg))
	require.NoError(t, err)

	require.NoError(t, m.Close())

	// A resolve after Close must serve the fallback without starting a watcher.
	addr, err := m.Resolve(context.Background(), "svc-a", "fallback:1")
	require.NoError(t, err)
	assert.Equal(t, "fallback:1", addr)

	assert.Zero(t, reg.watchCount(), "no watcher may be started after Close")

	// <=baseline (not ==): a leaked watcher is a persistent +1 that never satisfies
	// this, so Eventually fails and catches the leak; transient drainers converge.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline
	}, 5*time.Second, 20*time.Millisecond,
		"a resolve after Close leaked a watcher (baseline=%d, now=%d)", baseline, runtime.NumGoroutine())
}

// managedService builds a Service exactly as a registry read would return it: an
// authoritative External endpoint with the root routable endpoint mirrored into
// the deprecated flat fields (what Resolve/Service.Addr read).
func managedService(addr string, port int, scheme string) Service {
	svc := Service{External: &Endpoint{Address: addr, Port: port, Scheme: scheme}}
	svc.mirrorFlat()

	return svc
}

// TestManagedResolver_SecondCallDoesNotRequery is the core contract: the FIRST
// resolve seeds (one registry hit); the SECOND reads the cached Service without
// touching the registry. Against the old query-per-request code this fails
// (resolveCount == 2).
func TestManagedResolver_SecondCallDoesNotRequery(t *testing.T) {
	t.Parallel()

	reg := &countingRegistry{svc: managedService("10.0.0.1", 9000, "https")}
	m := enabledManager(t, reg)

	addr, err := m.Resolve(context.Background(), "svc-a", "")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:9000", addr)

	got, err := m.Resolve(context.Background(), "svc-a", "")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:9000", got)

	assert.Equal(t, 1, reg.resolveCount(),
		"second resolve must read the cache, not re-query the registry")
}

// TestManagedResolver_ConcurrentSameNameCollapsesToOneSeed proves that concurrent
// first-resolves of the SAME name collapse into a single seed (via sync.Once), so
// a burst of callers never stampedes the registry. Run under -race.
func TestManagedResolver_ConcurrentSameNameCollapsesToOneSeed(t *testing.T) {
	t.Parallel()

	reg := &countingRegistry{svc: managedService("10.0.0.1", 9000, "https")}
	m := enabledManager(t, reg)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, _ = m.Resolve(context.Background(), "svc-a", "")
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, reg.resolveCount(),
		"concurrent first-resolves of one name must collapse into a single seed")
}

// TestManagedResolver_DistinctNamesGetDistinctResolvers proves each name gets its
// own managed resolver (one seed each) rather than sharing a single cache entry.
func TestManagedResolver_DistinctNamesGetDistinctResolvers(t *testing.T) {
	t.Parallel()

	reg := &countingRegistry{svc: managedService("10.0.0.1", 9000, "https")}
	m := enabledManager(t, reg)

	_, err := m.Resolve(context.Background(), "svc-a", "")
	require.NoError(t, err)

	_, err = m.Resolve(context.Background(), "svc-b", "")
	require.NoError(t, err)

	assert.Equal(t, 2, reg.resolveCount(), "each distinct name must seed once")

	m.resolversMu.Lock()
	got := len(m.resolvers)
	m.resolversMu.Unlock()

	assert.Equal(t, 2, got, "distinct names must have distinct managed resolvers")
}

// TestManagedResolver_KeepsLastKnownGoodOnWatchFailure proves the request path
// keeps serving the last-known-good Service when a background watch update fails:
// a transient registry error must not blank a route consumers are still using.
func TestManagedResolver_KeepsLastKnownGoodOnWatchFailure(t *testing.T) {
	t.Parallel()

	reg := &countingRegistry{
		svc:     managedService("10.0.0.1", 9000, "https"),
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	// Seed a good value.
	addr, err := m.Resolve(context.Background(), "svc-a", "")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:9000", addr)

	// The registry now fails; a catalog event drives a failed background update.
	reg.set(Service{}, errors.New("consul down"))
	reg.watchCh <- Event{Type: EventDeregistered}

	// The cached (last-known-good) value must never change to empty/error.
	require.Never(t, func() bool {
		got, rErr := m.Resolve(context.Background(), "svc-a", "")

		return rErr != nil || got != "10.0.0.1:9000"
	}, 300*time.Millisecond, 20*time.Millisecond,
		"a failed watch update must keep the last-known-good value")
}

// TestManagedResolver_CloseStopsWatchers proves Manager.Close cancels the managed
// resolvers' background watch goroutines. It mirrors close_test.go: a quiet
// goroutine baseline, a live watcher raising the count, then a return to baseline
// after Close (settled via require.Eventually, no fixed sleep).
//
// Intentionally NOT parallel: runtime.NumGoroutine() is only stable when no other
// test runs concurrently.
func TestManagedResolver_CloseStopsWatchers(t *testing.T) {
	var prev, baseline int

	require.Eventually(t, func() bool {
		prev = baseline
		baseline = runtime.NumGoroutine()

		return baseline > 0 && baseline == prev
	}, time.Second, 20*time.Millisecond, "goroutine count did not stabilize for baseline")

	// An OPEN watch channel keeps runManagedUpdates parked (goroutine stays live).
	reg := &countingRegistry{
		svc:     managedService("10.0.0.1", 9000, "https"),
		watchCh: make(chan Event),
	}

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        log.NewNop(),
	}, WithRegistry(reg))
	require.NoError(t, err)

	_, err = m.Resolve(context.Background(), "svc-a", "")
	require.NoError(t, err)

	// The watcher goroutine must be live before Close, so the assertion is meaningful.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() > baseline
	}, time.Second, 20*time.Millisecond, "managed watch goroutine did not start")

	require.NoError(t, m.Close())

	// <=baseline settled via require.Eventually: every managed watch goroutine (and
	// its SafeGo wrapper) must return after Close. A watcher that fails to stop is a
	// persistent +1 that never satisfies <=baseline, so Eventually fails and the
	// leak is caught; goroutines draining from earlier tests are transient and the
	// condition converges, so a cold `-race` run does not flake. This is a real
	// bound, not a tolerance: it does not mask a persistent +1.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline
	}, 5*time.Second, 20*time.Millisecond,
		"managed watch goroutine did not stop after Close (baseline=%d, now=%d)",
		baseline, runtime.NumGoroutine())
}
