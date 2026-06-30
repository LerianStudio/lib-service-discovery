//go:build unit

package libsd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// flakyRegistry fails the first failsLeft Register calls, then succeeds and
// signals via done. It lets RegisterAsync's retry loop be observed deterministically.
type flakyRegistry struct {
	mu        sync.Mutex
	failsLeft int
	calls     int
	done      chan struct{}
}

func (r *flakyRegistry) Register(_ context.Context, _ Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls++

	if r.failsLeft > 0 {
		r.failsLeft--

		return errors.New("discovery server unavailable")
	}

	select {
	case <-r.done:
	default:
		close(r.done)
	}

	return nil
}

func (r *flakyRegistry) Deregister(_ context.Context, _ string) error { return nil }
func (r *flakyRegistry) Resolve(_ context.Context, _, _ string) (Service, error) {
	return Service{}, nil
}

func (r *flakyRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	ch := make(chan Event)
	close(ch)

	return ch, nil
}

func (r *flakyRegistry) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls
}

func TestRegisterAsync_RetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	reg := &flakyRegistry{failsLeft: 2, done: make(chan struct{})}
	m := enabledManager(t, reg)

	m.RegisterAsync(context.Background(), Service{ID: "svc-1", Name: "svc", Port: 8080})

	select {
	case <-reg.done:
	case <-time.After(3 * time.Second):
		t.Fatal("RegisterAsync did not succeed after retries")
	}

	if got := reg.callCount(); got != 3 {
		t.Fatalf("Register calls = %d, want 3 (2 failures + 1 success)", got)
	}
}

func TestRegisterAsync_NoopWhenDisabled(t *testing.T) {
	t.Parallel()

	reg := &flakyRegistry{done: make(chan struct{})}

	m, err := New(Config{Enabled: false}, WithRegistry(reg))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	m.RegisterAsync(context.Background(), Service{Name: "svc"})

	time.Sleep(50 * time.Millisecond)

	if got := reg.callCount(); got != 0 {
		t.Fatalf("Register calls = %d, want 0 (discovery disabled)", got)
	}
}

func TestRegisterAsync_NilManagerDoesNotPanic(t *testing.T) {
	t.Parallel()

	var m *Manager

	m.RegisterAsync(context.Background(), Service{Name: "svc"})
}

func TestRegisterAsync_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	// Always fails: the loop only ends when ctx is cancelled.
	reg := &flakyRegistry{failsLeft: 1 << 30, done: make(chan struct{})}
	m := enabledManager(t, reg)

	ctx, cancel := context.WithCancel(context.Background())
	m.RegisterAsync(ctx, Service{ID: "svc-1", Name: "svc", Port: 8080})

	// Let it attempt at least once, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	// After cancel the loop must stop; the call count should stabilize.
	time.Sleep(150 * time.Millisecond)
	first := reg.callCount()
	time.Sleep(300 * time.Millisecond)

	if got := reg.callCount(); got != first {
		t.Fatalf("Register kept being called after ctx cancel: %d -> %d", first, got)
	}
}
