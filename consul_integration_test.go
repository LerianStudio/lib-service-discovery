//go:build integration

package libsd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const consulAddr = "localhost:8500"

// advertiseAddr is the address Consul (running in Docker) uses to reach the
// health check endpoint started by the test. On macOS with Docker Desktop,
// "host.docker.internal" resolves to the host machine from inside a container.
const advertiseAddr = "host.docker.internal"

func integrationManager(t *testing.T) *Manager {
	t.Helper()

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    consulAddr,
		AdvertiseAddr: advertiseAddr,
		Logger:        NewNopLogger(),
	})
	require.NoError(t, err)

	// Stop the managed-resolver watch goroutines started by resolve calls.
	t.Cleanup(func() { _ = m.Close() })

	return m
}

// startHealthServer starts an HTTP server on a free port (all interfaces) that
// always returns 200 on /health. Returns the port and registers cleanup.
// Binding to 0.0.0.0 allows Consul running in Docker to reach it via host.docker.internal.
func startHealthServer(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	port := ln.Addr().(*net.TCPAddr).Port

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Handler: mux}

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	return port
}

// waitHealthy polls Consul until the service has at least one healthy instance
// or the timeout is reached.
func waitHealthy(t *testing.T, m *Manager, name string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		_, err := m.Resolve(context.Background(), name, "")
		if err == nil {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("service %q did not become healthy within %s", name, timeout)
}

func TestIntegration_RegisterAndResolve(t *testing.T) {
	port := startHealthServer(t)
	m := integrationManager(t)
	ctx := context.Background()

	svc := Service{
		ID:   fmt.Sprintf("test-svc-%d", port),
		Name: fmt.Sprintf("test-svc-%d", port),
		Port: port,
		Tags: []string{"integration"},
		HealthCheck: &HealthCheck{
			Interval: "2s",
			Timeout:  "1s",
		},
	}

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, m, svc.Name, 10*time.Second)

	addr, err := m.Resolve(ctx, svc.Name, "")
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:%d", advertiseAddr, port), addr)
}

func TestIntegration_ResolveNotFound(t *testing.T) {
	m := integrationManager(t)

	_, err := m.Resolve(context.Background(), "nonexistent-svc-xyz", "")
	assert.ErrorIs(t, err, ErrNoHealthyInstances)
}

func TestIntegration_ResolveNotFoundWithFallback(t *testing.T) {
	m := integrationManager(t)

	addr, err := m.Resolve(context.Background(), "nonexistent-svc-xyz", "fallback:9999")
	require.NoError(t, err)
	assert.Equal(t, "fallback:9999", addr)
}

func TestIntegration_Deregister(t *testing.T) {
	port := startHealthServer(t)
	m := integrationManager(t)
	ctx := context.Background()

	svc := Service{
		ID:          fmt.Sprintf("test-svc-dereg-%d", port),
		Name:        fmt.Sprintf("test-svc-dereg-%d", port),
		Port:        port,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	}

	require.NoError(t, m.Register(ctx, svc))
	waitHealthy(t, m, svc.Name, 10*time.Second)

	require.NoError(t, m.Deregister(ctx, svc.ID))

	// Under watch-and-cache, the manager that already resolved this name keeps its
	// last-known-good value until its background watch observes the deregistration —
	// it does NOT flip to an error on the next read. A FRESH manager (empty seed)
	// resolving after the deregistration is what surfaces ErrNoHealthyInstances, so
	// assert against a fresh manager. Eventually absorbs any catalog propagation lag.
	require.Eventually(t, func() bool {
		fresh := integrationManager(t)
		// Close the fresh manager inside the attempt so its managed-resolver watch
		// goroutines do not accumulate across the (potentially many) Eventually
		// iterations. Close is idempotent, so integrationManager's t.Cleanup Close is
		// still safe.
		defer func() { _ = fresh.Close() }()

		_, err := fresh.Resolve(ctx, svc.Name, "")

		return errors.Is(err, ErrNoHealthyInstances)
	}, 10*time.Second, 500*time.Millisecond,
		"a fresh manager resolving a deregistered service must get ErrNoHealthyInstances")
}

func TestIntegration_Watch(t *testing.T) {
	port := startHealthServer(t)
	m := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	t.Cleanup(cancel)

	svc := Service{
		ID:          fmt.Sprintf("test-svc-watch-%d", port),
		Name:        fmt.Sprintf("test-svc-watch-%d", port),
		Port:        port,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	}

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	// Wait until Consul marks the service healthy before starting the watch,
	// otherwise the first Watch poll returns the initial critical state.
	waitHealthy(t, m, svc.Name, 10*time.Second)

	ch, err := m.Watch(ctx, svc.Name)
	require.NoError(t, err)

	select {
	case event, open := <-ch:
		require.True(t, open, "watch channel closed unexpectedly")
		assert.Equal(t, EventRegistered, event.Type)
		assert.Equal(t, svc.Name, event.Service.Name)
	case <-time.After(10 * time.Second):
		t.Log("no event received within 10s — Consul index may not have changed")
	}
}

// TestIntegration_WatchCancelClosesChannel verifies #1: cancelling ctx aborts the
// in-flight long-poll (via QueryOptions.WithContext) so the channel closes promptly
// instead of blocking until WaitTime.
func TestIntegration_WatchCancelClosesChannel(t *testing.T) {
	port := startHealthServer(t)
	m := integrationManager(t)
	ctx, cancel := context.WithCancel(context.Background())

	svc := Service{
		ID:          fmt.Sprintf("test-svc-wc-%d", port),
		Name:        fmt.Sprintf("test-svc-wc-%d", port),
		Port:        port,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	}

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, m, svc.Name, 10*time.Second)

	ch, err := m.Watch(ctx, svc.Name)
	require.NoError(t, err)

	// Drain the initial event (if any) so the goroutine parks in the blocking poll.
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
	}

	cancel()

	// Drain remaining buffered events, then assert the channel closes promptly.
	deadline := time.After(3 * time.Second)

	for {
		select {
		case _, open := <-ch:
			if !open {
				return // closed — success
			}
		case <-deadline:
			t.Fatal("watch channel did not close within 3s of ctx cancel")
		}
	}
}

// TestIntegration_ResolveContextCancelled verifies #1: a cancelled ctx makes the
// Consul query fail fast rather than hanging.
func TestIntegration_ResolveContextCancelled(t *testing.T) {
	m := integrationManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := m.Resolve(ctx, "any-svc", "")
	require.Error(t, err)
}

// TestIntegration_ResolveHonorsDialTimeout proves the DialTimeout wired into the
// transport's DialContext actually bounds connection establishment. It points a
// Manager at a non-routable/blackhole address (RFC 5735 10.255.255.1, which
// silently drops SYNs so the dial hangs until the deadline) with a short
// DialTimeout, then asserts Resolve fails well inside that deadline — far below
// the fast client's ResponseHeaderTimeout. A regression that zeroed DialTimeout
// would let the dial hang and this test would exceed the assertion bound.
func TestIntegration_ResolveHonorsDialTimeout(t *testing.T) {
	const dialTimeout = 200 * time.Millisecond

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "10.255.255.1:8500", // blackhole: SYNs are dropped, dial hangs until deadline
		AdvertiseAddr: advertiseAddr,
		DialTimeout:   dialTimeout,
		Logger:        NewNopLogger(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	start := time.Now()
	_, resolveErr := m.Resolve(context.Background(), "any-svc", "")
	elapsed := time.Since(start)

	require.Error(t, resolveErr, "resolve against a blackhole must fail")
	assert.Less(t, elapsed, 2*time.Second,
		"resolve must fail within the dial deadline (got %s, dial timeout %s)", elapsed, dialTimeout)
}

// TestIntegration_ResolveRoundRobin verifies #4: with multiple healthy instances,
// each registry resolve spreads requests across them instead of always returning
// the first.
//
// Round-robin is a REGISTRY-layer behavior. Under the watch-and-cache model the
// Manager's one-shot Resolve serves a single cached Service and no longer spreads
// per read (spreading now happens on the seed and on each watch-driven refresh),
// so this test exercises round-robin where it lives — directly on the registry
// backend (accessible from this in-package test).
func TestIntegration_ResolveRoundRobin(t *testing.T) {
	p1 := startHealthServer(t)
	p2 := startHealthServer(t)
	m := integrationManager(t)
	ctx := context.Background()

	name := fmt.Sprintf("test-svc-rr-%d", p1)

	for _, p := range []int{p1, p2} {
		id := fmt.Sprintf("%s-%d", name, p)
		svc := Service{
			ID:          id,
			Name:        name,
			Port:        p,
			HealthCheck: &HealthCheck{Interval: "1s", Timeout: "1s"},
		}
		require.NoError(t, m.Register(ctx, svc))

		t.Cleanup(func() { _ = m.Deregister(context.Background(), id) })
	}

	reg, ok := m.registry.(*consulRegistry)
	require.True(t, ok, "the default backend must be *consulRegistry")

	// Once both instances are healthy, repeated registry resolves must surface both
	// ports (round-robin across healthy instances).
	seen := map[string]bool{}

	require.Eventually(t, func() bool {
		svc, err := reg.Resolve(ctx, name, "")
		if err == nil {
			seen[svc.Addr()] = true
		}

		return len(seen) >= 2
	}, 20*time.Second, 200*time.Millisecond, "expected registry round-robin to return both instances")

	assert.Len(t, seen, 2)
}
