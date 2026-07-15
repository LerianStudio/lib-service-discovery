//go:build unit

package libsd

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heartbeatStubRegistry mimics consulRegistry's heartbeat lifecycle: each
// Register spawns a background goroutine bound to a per-service context, tracks
// its cancel func, and Close() cancels all of them. It lets a unit test assert
// that Manager.Close() actually stops the background goroutines (observable via
// a WaitGroup) without a live Consul.
type heartbeatStubRegistry struct {
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
	wg      sync.WaitGroup
	closes  int
}

func newHeartbeatStubRegistry() *heartbeatStubRegistry {
	return &heartbeatStubRegistry{cancels: make(map[string]context.CancelFunc)}
}

func (r *heartbeatStubRegistry) Register(_ context.Context, svc Service) error {
	ctx, cancel := context.WithCancel(context.Background())

	r.mu.Lock()
	r.cancels[svc.ID] = cancel
	r.mu.Unlock()

	r.wg.Add(1)

	go func() {
		defer r.wg.Done()

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return nil
}

func (r *heartbeatStubRegistry) Deregister(_ context.Context, _ string) error { return nil }

func (r *heartbeatStubRegistry) Resolve(_ context.Context, _, _ string) (Service, error) {
	return Service{}, nil
}

func (r *heartbeatStubRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	ch := make(chan Event)
	close(ch)

	return ch, nil
}

func (r *heartbeatStubRegistry) Close() error {
	r.mu.Lock()
	r.closes++
	for id, cancel := range r.cancels {
		cancel()
		delete(r.cancels, id)
	}
	r.mu.Unlock()

	return nil
}

// ── Epic 4.3: Manager.Close ─────────────────────────────────────────────────────

func TestManagerClose_StopsHeartbeatGoroutines(t *testing.T) {
	t.Parallel()

	reg := newHeartbeatStubRegistry()
	m := enabledManager(t, reg)

	require.NoError(t, m.Register(context.Background(), Service{ID: "svc-a-1", Name: "svc-a"}))
	m.RegisterAsync(context.Background(), Service{ID: "svc-b-1", Name: "svc-b"})

	// Let RegisterAsync's goroutine invoke Register.
	assert.Eventually(t, func() bool {
		reg.mu.Lock()
		defer reg.mu.Unlock()

		return len(reg.cancels) == 2
	}, time.Second, time.Millisecond)

	require.NoError(t, m.Close())

	// All background goroutines must have returned.
	done := make(chan struct{})
	go func() {
		reg.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat goroutines did not stop after Close()")
	}
}

// TestManagerClose_ReturnsToGoroutineBaseline complements the WaitGroup-based
// proof above with a dependency-free leak check: it records runtime.NumGoroutine()
// before registering, then asserts the count returns to (or below) that baseline
// after Close(). PROJECT_RULES keeps dependencies minimal, so this deliberately
// avoids goleak.
//
// Intentionally NOT parallel: runtime.NumGoroutine() is only stable when no other
// test runs concurrently. Go parks every t.Parallel() test until the sequential
// tests finish, so a sequential test observes a quiet runtime. The settle after
// Close is done via require.Eventually (no fixed sleep).
func TestManagerClose_ReturnsToGoroutineBaseline(t *testing.T) {
	// Establish a quiet baseline: two consecutive equal reads (spaced by the
	// Eventually tick) mean transient goroutines from earlier tests have drained.
	var prev, baseline int

	require.Eventually(t, func() bool {
		prev = baseline
		baseline = runtime.NumGoroutine()

		return baseline > 0 && baseline == prev
	}, time.Second, 20*time.Millisecond, "goroutine count did not stabilize for baseline")

	reg := newHeartbeatStubRegistry()
	m := enabledManager(t, reg)

	require.NoError(t, m.Register(context.Background(), Service{ID: "svc-a-1", Name: "svc-a"}))
	m.RegisterAsync(context.Background(), Service{ID: "svc-b-1", Name: "svc-b"})

	// Both heartbeat goroutines must be live before Close, so the count genuinely
	// rose above baseline and the post-Close assertion is meaningful.
	require.Eventually(t, func() bool {
		reg.mu.Lock()
		defer reg.mu.Unlock()

		return len(reg.cancels) == 2
	}, time.Second, time.Millisecond)

	require.NoError(t, m.Close())

	// After Close, every heartbeat/async goroutine must return, bringing the count
	// back to baseline. A small tolerance absorbs runtime/GC bookkeeping goroutines.
	const tolerance = 2

	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline+tolerance
	}, 2*time.Second, 20*time.Millisecond,
		"goroutines did not return to baseline after Close (baseline=%d)", baseline)
}

func TestManagerClose_Idempotent(t *testing.T) {
	t.Parallel()

	reg := newHeartbeatStubRegistry()
	m := enabledManager(t, reg)

	require.NoError(t, m.Register(context.Background(), Service{ID: "svc-a-1", Name: "svc-a"}))

	require.NoError(t, m.Close())
	assert.NotPanics(t, func() { _ = m.Close() })
	require.NoError(t, m.Close())
}

func TestManagerClose_NilReceiverSafe(t *testing.T) {
	t.Parallel()

	var m *Manager

	assert.NotPanics(t, func() {
		assert.NoError(t, m.Close())
	})
}

func TestManagerClose_RegistryWithoutCloser(t *testing.T) {
	t.Parallel()

	// stubRegistry does not implement Close(); Manager.Close must be a safe no-op.
	m := enabledManager(t, &stubRegistry{})

	assert.NotPanics(t, func() {
		assert.NoError(t, m.Close())
	})
}

func TestManagerClose_DisabledManagerSafe(t *testing.T) {
	t.Parallel()

	m, err := New(Config{Enabled: false, Logger: log.NewNop()})
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		assert.NoError(t, m.Close())
	})
}

// ── consulRegistry.Close (direct) ───────────────────────────────────────────────

func TestConsulRegistryClose_CancelsHeartbeatsAndIsIdempotent(t *testing.T) {
	t.Parallel()

	var canceled int

	r := &consulRegistry{
		logger: log.NewNop(),
		heartbeats: map[string]context.CancelFunc{
			"svc-a-1": func() { canceled++ },
			"svc-b-1": func() { canceled++ },
		},
	}

	require.NoError(t, r.Close())
	assert.Equal(t, 2, canceled, "every heartbeat cancel must be invoked")

	r.mu.Lock()
	assert.Empty(t, r.heartbeats, "heartbeats map must be drained")
	r.mu.Unlock()

	// Idempotent: a second Close is a safe no-op.
	assert.NotPanics(t, func() { _ = r.Close() })
	assert.Equal(t, 2, canceled)

	// Nil-receiver safe.
	var nilReg *consulRegistry
	assert.NotPanics(t, func() { _ = nilReg.Close() })
}
