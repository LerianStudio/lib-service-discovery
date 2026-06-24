//go:build unit

package libsd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// watchableRegistry is a Registry stub whose Watch channel and Resolve result
// are controllable from the test, so we can simulate catalog changes.
type watchableRegistry struct {
	mu      sync.Mutex
	svc     Service
	err     error
	watchCh chan Event
}

func (r *watchableRegistry) Register(_ context.Context, _ Service) error  { return nil }
func (r *watchableRegistry) Deregister(_ context.Context, _ string) error { return nil }

func (r *watchableRegistry) Resolve(_ context.Context, _, _ string) (Service, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.svc, r.err
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
		svc:     Service{Address: "10.0.1.5", Port: 3002},
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
		svc:     Service{Address: "10.0.1.5", Port: 3002},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	require.Equal(t, "10.0.1.5:3002", dr.Address())

	// Provider re-registers under a new address; a catalog-change event fires.
	reg.setResolve(Service{Address: "10.0.2.9", Port: 3002}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	require.Eventually(t, func() bool {
		return dr.Address() == "10.0.2.9:3002"
	}, 2*time.Second, 10*time.Millisecond, "resolver did not pick up the new address")
}

func TestWatchResolveService_SeedsAddressAndScheme(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{Address: "10.0.1.5", Port: 3002, Scheme: "https"},
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
		svc:     Service{Address: "10.0.1.5", Port: 3002, Scheme: "http"},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolveService(context.Background(), "midaz-ledger", Service{})
	require.NoError(t, err)

	t.Cleanup(dr.Stop)

	require.Equal(t, "http", dr.Scheme())

	// Provider re-registers behind TLS; the resolver must follow the new scheme.
	reg.setResolve(Service{Address: "10.0.2.9", Port: 3002, Scheme: "https"}, nil)
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

func TestWatchResolve_StopHaltsUpdates(t *testing.T) {
	t.Parallel()

	reg := &watchableRegistry{
		svc:     Service{Address: "10.0.1.5", Port: 3002},
		watchCh: make(chan Event, 1),
	}
	m := enabledManager(t, reg)

	dr, err := m.WatchResolve(context.Background(), "midaz-ledger", "")
	require.NoError(t, err)

	dr.Stop()
	time.Sleep(100 * time.Millisecond) // let the goroutine observe ctx.Done and exit

	// With the watcher stopped, an event enqueues on the buffered channel but is
	// never consumed, so the cached address must stay unchanged.
	reg.setResolve(Service{Address: "10.0.2.9", Port: 3002}, nil)
	reg.watchCh <- Event{Type: EventRegistered}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, "10.0.1.5:3002", dr.Address())
}
