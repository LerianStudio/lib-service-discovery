//go:build unit

package libsd

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

// fakeConsul is an httptest-backed stand-in for the Consul HTTP API, exercising
// the real consulRegistry wire paths (register / deregister / TTL update /
// health) without a live Consul. It runs in unit CI.
type fakeConsul struct {
	mu             sync.Mutex
	registerCount  int
	deregisterIDs  []string
	updateTTLCount int

	// ttlFailFirst makes the first N UpdateTTL calls return 404 (unknown check),
	// driving the self-heal re-registration path.
	ttlFailFirst int

	// entries is served by /v1/health/service/{name}; index is the X-Consul-Index.
	entries []*api.ServiceEntry
	index   uint64

	server *httptest.Server
}

func newFakeConsul(t *testing.T) *fakeConsul {
	t.Helper()

	f := &fakeConsul{index: 1}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)

	return f
}

func (f *fakeConsul) addr() string { return strings.TrimPrefix(f.server.URL, "http://") }

func (f *fakeConsul) newRegistry(t *testing.T) Registry {
	t.Helper()

	r, err := newConsulRegistry(Config{ConsulAddr: f.addr()}, nil)
	if err != nil {
		t.Fatalf("newConsulRegistry: %v", err)
	}

	return r
}

func (f *fakeConsul) counts() (reg, ttl int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.registerCount, f.updateTTLCount
}

func (f *fakeConsul) handle(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	switch {
	case r.Method == http.MethodPut && path == "/v1/agent/service/register":
		f.mu.Lock()
		f.registerCount++
		f.mu.Unlock()

		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPut && strings.HasPrefix(path, "/v1/agent/service/deregister/"):
		id := strings.TrimPrefix(path, "/v1/agent/service/deregister/")

		f.mu.Lock()
		f.deregisterIDs = append(f.deregisterIDs, id)
		f.mu.Unlock()

		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPut && strings.HasPrefix(path, "/v1/agent/check/update/"):
		f.mu.Lock()
		f.updateTTLCount++
		fail := f.updateTTLCount <= f.ttlFailFirst
		f.mu.Unlock()

		if fail {
			http.Error(w, `Unknown check ID "`+strings.TrimPrefix(path, "/v1/agent/check/update/")+`"`, http.StatusNotFound)

			return
		}

		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v1/health/service/"):
		f.mu.Lock()
		entries := f.entries
		idx := f.index
		f.mu.Unlock()

		// Loosely honor the blocking query so Watch doesn't busy-spin in tests.
		if wi := r.URL.Query().Get("index"); wi != "" {
			if n, _ := strconv.ParseUint(wi, 10, 64); n >= idx {
				time.Sleep(20 * time.Millisecond)
			}
		}

		w.Header().Set("X-Consul-Index", strconv.FormatUint(idx, 10))
		w.Header().Set("Content-Type", "application/json")

		if entries == nil {
			entries = []*api.ServiceEntry{}
		}

		_ = json.NewEncoder(w).Encode(entries)

	default:
		w.WriteHeader(http.StatusOK)
	}
}

func checkedEntry(id, addr string, port int, status string) *api.ServiceEntry {
	return &api.ServiceEntry{
		Service: &api.AgentService{ID: id, Service: "svc", Address: addr, Port: port},
		Checks:  api.HealthChecks{{Status: status}},
	}
}

func TestConsulRegister_TTLStartsHeartbeat(t *testing.T) {
	t.Parallel()

	f := newFakeConsul(t)
	r := f.newRegistry(t)

	err := r.Register(context.Background(), Service{
		ID:          "svc-1",
		Name:        "svc",
		Port:        8080,
		HealthCheck: &HealthCheck{TTL: "30s"},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Cleanup(func() { _ = r.Deregister(context.Background(), "svc-1") })

	// The first heartbeat ("pass") runs synchronously inside Register.
	reg, ttl := f.counts()
	if reg != 1 {
		t.Fatalf("registerCount = %d, want 1", reg)
	}

	if ttl != 1 {
		t.Fatalf("updateTTLCount = %d, want 1 (synchronous first pass)", ttl)
	}
}

func TestConsulRegister_SelfHealOn404(t *testing.T) {
	t.Parallel()

	f := newFakeConsul(t)
	f.ttlFailFirst = 1 // first heartbeat 404s -> triggers re-register
	r := f.newRegistry(t)

	err := r.Register(context.Background(), Service{
		ID:          "svc-1",
		Name:        "svc",
		Port:        8080,
		HealthCheck: &HealthCheck{TTL: "30s"},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Cleanup(func() { _ = r.Deregister(context.Background(), "svc-1") })

	// First pass: UpdateTTL -> 404 -> re-register -> UpdateTTL -> 200.
	reg, ttl := f.counts()
	if reg != 2 {
		t.Fatalf("registerCount = %d, want 2 (initial + self-heal re-register)", reg)
	}

	if ttl != 2 {
		t.Fatalf("updateTTLCount = %d, want 2 (failed + retried after re-register)", ttl)
	}
}

func TestConsulResolve_RoundRobin(t *testing.T) {
	t.Parallel()

	f := newFakeConsul(t)
	f.entries = []*api.ServiceEntry{
		checkedEntry("svc-1", "10.0.0.1", 1, api.HealthPassing),
		checkedEntry("svc-2", "10.0.0.2", 2, api.HealthPassing),
	}
	r := f.newRegistry(t)

	s1, err := r.Resolve(context.Background(), "svc", "")
	if err != nil {
		t.Fatalf("Resolve #1: %v", err)
	}

	s2, err := r.Resolve(context.Background(), "svc", "")
	if err != nil {
		t.Fatalf("Resolve #2: %v", err)
	}

	if s1.Address == s2.Address {
		t.Fatalf("round-robin returned same instance twice: %q", s1.Address)
	}
}

func TestConsulResolve_NoInstances(t *testing.T) {
	t.Parallel()

	f := newFakeConsul(t)
	f.entries = nil
	r := f.newRegistry(t)

	_, err := r.Resolve(context.Background(), "svc", "")
	if !errors.Is(err, ErrNoHealthyInstances) {
		t.Fatalf("err = %v, want ErrNoHealthyInstances", err)
	}
}

func TestConsulDeregister_StopsAndCalls(t *testing.T) {
	t.Parallel()

	f := newFakeConsul(t)
	r := f.newRegistry(t)

	if err := r.Register(context.Background(), Service{
		ID:          "svc-1",
		Name:        "svc",
		Port:        8080,
		HealthCheck: &HealthCheck{TTL: "30s"},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := r.Deregister(context.Background(), "svc-1"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.deregisterIDs) != 1 || f.deregisterIDs[0] != "svc-1" {
		t.Fatalf("deregisterIDs = %v, want [svc-1]", f.deregisterIDs)
	}
}

func TestConsulWatch_EmitsRegistered(t *testing.T) {
	t.Parallel()

	f := newFakeConsul(t)
	f.entries = []*api.ServiceEntry{checkedEntry("svc-1", "10.0.0.1", 8080, api.HealthPassing)}
	f.index = 10
	r := f.newRegistry(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := r.Watch(ctx, "svc")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Type != EventRegistered {
			t.Fatalf("event type = %q, want %q", ev.Type, EventRegistered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}

func TestConsulWatch_EmitsDeregisteredOnCritical(t *testing.T) {
	t.Parallel()

	f := newFakeConsul(t)
	f.entries = []*api.ServiceEntry{checkedEntry("svc-1", "10.0.0.1", 8080, api.HealthCritical)}
	f.index = 10
	r := f.newRegistry(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := r.Watch(ctx, "svc")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Type != EventDeregistered {
			t.Fatalf("event type = %q, want %q", ev.Type, EventDeregistered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}
