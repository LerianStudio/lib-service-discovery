//go:build unit

package libsd

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v2/log"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func healthEntry(id, name, addr string, port int, scheme string) *api.ServiceEntry {
	meta := map[string]string{}
	if scheme != "" {
		meta["scheme"] = scheme
	}

	return &api.ServiceEntry{
		Service: &api.AgentService{
			ID:      id,
			Service: name,
			Address: addr,
			Port:    port,
			Tags:    []string{"region=us"},
			Meta:    meta,
		},
	}
}

// ── serviceFromEntry (#7: scheme surfaced consistently) ─────────────────────────

func TestServiceFromEntry_MapsSchemeFromMeta(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(healthEntry("id-1", "svc-a", "10.0.0.1", 8080, "https"))

	assert.Equal(t, "id-1", svc.ID)
	assert.Equal(t, "svc-a", svc.Name)
	assert.Equal(t, "10.0.0.1", svc.Address)
	assert.Equal(t, 8080, svc.Port)
	assert.Equal(t, "https", svc.Scheme)
	assert.Equal(t, []string{"region=us"}, svc.Tags)
}

func TestServiceFromEntry_EmptySchemeWhenAbsent(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(healthEntry("id-1", "svc-a", "10.0.0.1", 8080, ""))

	assert.Equal(t, "", svc.Scheme)
}

// ── serviceFromEntry (Task 1.4.1: reconstruct Service.Internal from Meta) ────────

// entryWithMeta builds a health entry whose service Meta is exactly meta (nil is
// preserved as a nil Meta, exercising the nil-map read path).
func entryWithMeta(meta map[string]string) *api.ServiceEntry {
	return &api.ServiceEntry{
		Service: &api.AgentService{
			ID:      "id-1",
			Service: "svc-a",
			Address: "10.0.0.1",
			Port:    8080,
			Tags:    []string{"region=us"},
			Meta:    meta,
		},
	}
}

// (a) All internal keys present -> Internal populated with exact Address/Port/Scheme.
func TestServiceFromEntry_ReconstructsInternalEndpoint(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(entryWithMeta(map[string]string{
		"scheme":           "https",
		"internal_address": "svc-a.ns.svc.cluster.local",
		"internal_port":    "8080",
		"internal_scheme":  "http",
	}))

	// internal_* keys but no external_* keys → internal-only: External stays nil and
	// the flat mirror reflects the routable internal endpoint (root == Internal),
	// NOT the raw entry root address.
	assert.Nil(t, svc.External, "internal-only entry -> External nil")
	assert.Equal(t, "svc-a.ns.svc.cluster.local", svc.Address)
	assert.Equal(t, 8080, svc.Port)
	assert.Equal(t, "http", svc.Scheme)

	if assert.NotNil(t, svc.Internal, "internal_address present -> Internal must be populated") {
		assert.Equal(t, "svc-a.ns.svc.cluster.local", svc.Internal.Address)
		assert.Equal(t, 8080, svc.Internal.Port)
		assert.Equal(t, "http", svc.Internal.Scheme)
	}
}

// internal_scheme absent -> Scheme "" but Internal still populated.
func TestServiceFromEntry_InternalWithoutScheme(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(entryWithMeta(map[string]string{
		"internal_address": "10.0.0.9",
		"internal_port":    "9090",
	}))

	if assert.NotNil(t, svc.Internal) {
		assert.Equal(t, "10.0.0.9", svc.Internal.Address)
		assert.Equal(t, 9090, svc.Internal.Port)
		assert.Equal(t, "", svc.Internal.Scheme)
	}
}

// (b) No internal keys -> Internal stays nil (provider not migrated).
func TestServiceFromEntry_NoInternalKeysInternalNil(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(entryWithMeta(map[string]string{"scheme": "https"}))

	assert.Nil(t, svc.Internal, "no internal_address -> Internal must stay nil")
}

// internal_address present but empty -> treated as absent, Internal stays nil.
func TestServiceFromEntry_EmptyInternalAddressInternalNil(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(entryWithMeta(map[string]string{"internal_address": ""}))

	assert.Nil(t, svc.Internal, "empty internal_address -> Internal must stay nil")
}

// (c) internal_port invalid -> Internal set with Port 0, no panic.
func TestServiceFromEntry_InvalidInternalPortIsZero(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(entryWithMeta(map[string]string{
		"internal_address": "10.0.0.9",
		"internal_port":    "abc",
		"internal_scheme":  "http",
	}))

	if assert.NotNil(t, svc.Internal) {
		assert.Equal(t, "10.0.0.9", svc.Internal.Address)
		assert.Equal(t, 0, svc.Internal.Port, "invalid internal_port -> Port 0, no panic")
		assert.Equal(t, "http", svc.Internal.Scheme)
	}
}

// (d) nil Meta -> Internal nil, no panic (map read on nil returns zero value).
func TestServiceFromEntry_NilMetaInternalNil(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(entryWithMeta(nil))

	assert.Nil(t, svc.Internal, "nil Meta -> Internal nil, no panic")
}

// ROUND-TRIP: a Service serialized via serviceMeta and read back via
// serviceFromEntry reconstructs an identical Internal endpoint.
func TestServiceFromEntry_RoundTripInternal(t *testing.T) {
	t.Parallel()

	original := &Endpoint{
		Address: "svc-a.ns.svc.cluster.local",
		Port:    9443,
		Scheme:  "https",
	}

	meta := serviceMeta(Service{
		Scheme:   "https",
		Meta:     map[string]string{"team": "core"},
		Internal: original,
	})

	svc := serviceFromEntry(entryWithMeta(meta))

	if assert.NotNil(t, svc.Internal, "round-trip must reconstruct Internal") {
		assert.Equal(t, *original, *svc.Internal, "round-tripped Internal must equal the original")
	}
}

// ── serviceFromEntry (Epic 3.3: symmetric external_* reconstruction) ─────────────

// New-build round trip: an entry carrying BOTH external_* and internal_* keys
// reconstructs External from the external_* keys (NOT the raw entry address) and
// Internal from the internal_* keys.
func TestServiceFromEntry_ReconstructsBothEndpoints(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(&api.ServiceEntry{
		Service: &api.AgentService{
			ID:      "id-1",
			Service: "svc-a",
			Address: "10.0.0.1", // raw entry addr must be ignored when external_* present
			Port:    8080,
			Meta: map[string]string{
				"external_address": "fees.example.net",
				"external_port":    "443",
				"external_scheme":  "https",
				"scheme":           "https",
				"internal_address": "svc-a.ns.svc.cluster.local",
				"internal_port":    "9090",
				"internal_scheme":  "http",
			},
		},
	})

	if assert.NotNil(t, svc.External) {
		assert.Equal(t, Endpoint{Address: "fees.example.net", Port: 443, Scheme: "https"}, *svc.External)
	}

	if assert.NotNil(t, svc.Internal) {
		assert.Equal(t, Endpoint{Address: "svc-a.ns.svc.cluster.local", Port: 9090, Scheme: "http"}, *svc.Internal)
	}

	// The flat mirror reflects External.
	assert.Equal(t, "fees.example.net", svc.Address)
	assert.Equal(t, 443, svc.Port)
	assert.Equal(t, "https", svc.Scheme)
}

// Internal-only new registration: reg.Address is the internal endpoint and there
// are no external_* keys. Internal is reconstructed from internal_*; External stays
// nil (no external endpoint was ever advertised), while the deprecated flat mirror
// reflects the routable internal endpoint so legacy Resolve callers still work.
func TestServiceFromEntry_InternalOnlyReconstructsInternal(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(&api.ServiceEntry{
		Service: &api.AgentService{
			ID:      "id-1",
			Service: "svc-a",
			Address: "svc-a.ns.svc.cluster.local", // reg.Address == internal for internal-only
			Port:    9090,
			Meta: map[string]string{
				"internal_address": "svc-a.ns.svc.cluster.local",
				"internal_port":    "9090",
				"internal_scheme":  "http",
			},
		},
	})

	assert.Nil(t, svc.External, "internal-only entry must leave External nil (no external endpoint advertised)")

	if assert.NotNil(t, svc.Internal) {
		assert.Equal(t, Endpoint{Address: "svc-a.ns.svc.cluster.local", Port: 9090, Scheme: "http"}, *svc.Internal)
	}

	// The flat mirror reflects the routable internal endpoint (root == Internal).
	assert.Equal(t, "svc-a.ns.svc.cluster.local", svc.Address)
	assert.Equal(t, 9090, svc.Port)
	assert.Equal(t, "http", svc.Scheme)
}

// ROUND-TRIP (real read path): an internal-only Service serialized via serviceMeta
// and read back via serviceFromEntry — with the entry root Address set to the
// internal endpoint exactly as Register does — must reconstruct External == nil,
// Internal populated, and EndpointFor(External) == ErrEndpointViewUnavailable,
// while the flat mirror (legacy Resolve) yields the routable internal address.
func TestServiceFromEntry_RoundTripInternalOnly(t *testing.T) {
	t.Parallel()

	internal := &Endpoint{Address: "svc-a.ns.svc.cluster.local", Port: 9090, Scheme: "http"}

	meta := serviceMeta(Service{
		Meta:     map[string]string{"team": "core"},
		Internal: internal,
	})

	// Register serializes the internal endpoint as the entry root for internal-only.
	svc := serviceFromEntry(&api.ServiceEntry{
		Service: &api.AgentService{
			ID:      "id-1",
			Service: "svc-a",
			Address: internal.Address,
			Port:    internal.Port,
			Meta:    meta,
		},
	})

	assert.Nil(t, svc.External, "internal-only round-trip must leave External nil")

	if assert.NotNil(t, svc.Internal) {
		assert.Equal(t, *internal, *svc.Internal)
	}

	_, err := svc.EndpointFor(External)
	assert.ErrorIs(t, err, ErrEndpointViewUnavailable,
		"External view against an internal-only provider must be unavailable")

	// Legacy Resolve reads the flat mirror: it must be the routable internal addr.
	assert.Equal(t, "svc-a.ns.svc.cluster.local:9090", svc.Addr(),
		"legacy Resolve must return the routable internal address, never :0")
}

// ROUND-TRIP (real read path): an external-only Service round-trips with External
// populated and Internal nil.
func TestServiceFromEntry_RoundTripExternalOnly(t *testing.T) {
	t.Parallel()

	external := &Endpoint{Address: "fees.example.net", Port: 443, Scheme: "https"}

	meta := serviceMeta(Service{External: external})

	svc := serviceFromEntry(&api.ServiceEntry{
		Service: &api.AgentService{
			ID:      "id-1",
			Service: "svc-a",
			Address: external.Address,
			Port:    external.Port,
			Meta:    meta,
		},
	})

	assert.Nil(t, svc.Internal, "external-only round-trip must leave Internal nil")

	if assert.NotNil(t, svc.External) {
		assert.Equal(t, *external, *svc.External)
	}
}

// Back-compat: an OLD registration wrote only the entry Address/Port and
// Meta["scheme"] (no external_* keys). External is reconstructed from those, and
// Internal stays nil.
func TestServiceFromEntry_BackCompatOldRegistration(t *testing.T) {
	t.Parallel()

	svc := serviceFromEntry(&api.ServiceEntry{
		Service: &api.AgentService{
			ID:      "id-1",
			Service: "svc-a",
			Address: "legacy.example.net",
			Port:    8443,
			Meta:    map[string]string{"scheme": "https"},
		},
	})

	if assert.NotNil(t, svc.External, "back-compat must reconstruct External from the entry address") {
		assert.Equal(t, Endpoint{Address: "legacy.example.net", Port: 8443, Scheme: "https"}, *svc.External)
	}

	assert.Nil(t, svc.Internal, "no internal_* keys -> Internal stays nil")
}

// ROUND-TRIP: a Service serialized via serviceMeta and read back via
// serviceFromEntry reconstructs identical External AND Internal endpoints.
func TestServiceFromEntry_RoundTripBothEndpoints(t *testing.T) {
	t.Parallel()

	external := &Endpoint{Address: "fees.example.net", Port: 443, Scheme: "https"}
	internal := &Endpoint{Address: "svc-a.ns.svc.cluster.local", Port: 9090, Scheme: "http"}

	meta := serviceMeta(Service{
		Meta:     map[string]string{"team": "core"},
		External: external,
		Internal: internal,
	})

	svc := serviceFromEntry(entryWithMeta(meta))

	if assert.NotNil(t, svc.External) {
		assert.Equal(t, *external, *svc.External, "round-tripped External must equal the original")
	}

	if assert.NotNil(t, svc.Internal) {
		assert.Equal(t, *internal, *svc.Internal, "round-tripped Internal must equal the original")
	}
}

// ── serviceMeta (Epic 3.3: serialize the external endpoint) ──────────────────────

func TestServiceMeta_SerializesExternalEndpoint(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{
		Meta:     map[string]string{"team": "core"},
		External: &Endpoint{Address: "fees.example.net", Port: 443, Scheme: "https"},
	})

	assert.Equal(t, "fees.example.net", got["external_address"])
	assert.Equal(t, "443", got["external_port"])
	assert.Equal(t, "https", got["external_scheme"])
	assert.Equal(t, "https", got["scheme"], "the scheme key mirrors the external scheme")
	assert.Equal(t, "core", got["team"])
}

// External endpoint with an empty scheme: external_address/external_port are
// written, but external_scheme and the scheme mirror are omitted.
func TestServiceMeta_ExternalWithoutScheme(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{External: &Endpoint{Address: "10.0.0.5", Port: 8080}})

	assert.Equal(t, "10.0.0.5", got["external_address"])
	assert.Equal(t, "8080", got["external_port"])

	_, hasExtScheme := got["external_scheme"]
	assert.False(t, hasExtScheme, "empty external scheme -> external_scheme omitted")

	_, hasScheme := got["scheme"]
	assert.False(t, hasScheme, "empty external scheme -> scheme mirror omitted")
}

// Internal-only Service: internal_* keys are written; no external_* keys.
func TestServiceMeta_InternalOnlyNoExternalKeys(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{
		Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
	})

	assert.Equal(t, "svc.ns.svc.cluster.local", got["internal_address"])
	assert.Equal(t, "9090", got["internal_port"])
	assert.Equal(t, "http", got["internal_scheme"])

	_, hasExtAddr := got["external_address"]
	assert.False(t, hasExtAddr, "internal-only -> no external_address key")
}

// ── startHeartbeat after Close (#15: no heartbeat survives shutdown) ─────────────

// TestConsulRegistryStartHeartbeat_NoopAfterClose proves #15(b): once Close has
// run, startHeartbeat refuses to insert a new heartbeat, so a Register that raced
// shutdown (e.g. a pending RegisterAsync retry) cannot resurrect a background
// goroutine that escapes Close's cleanup. Because the closed guard returns before
// pass() (which would deref the nil client), the no-op is observable without a
// live Consul.
func TestConsulRegistryStartHeartbeat_NoopAfterClose(t *testing.T) {
	t.Parallel()

	r := &consulRegistry{
		logger:     log.NewNop(),
		heartbeats: map[string]context.CancelFunc{},
	}

	require.NoError(t, r.Close())

	// A heartbeat inserted after Close must be refused: no goroutine, empty map, no
	// panic from touching the (nil) client.
	assert.NotPanics(t, func() {
		r.startHeartbeat(&api.AgentServiceRegistration{ID: "svc-late"}, "30s")
	})

	r.mu.Lock()
	assert.Empty(t, r.heartbeats, "startHeartbeat must be a no-op after Close")
	r.mu.Unlock()
}

// ── atoiSafe (tolerant parse: 0 on error, never panics) ──────────────────────────

func TestAtoiSafe(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want int
	}{
		{"valid", "8080", 8080},
		{"empty", "", 0},
		{"non-numeric", "abc", 0},
		{"negative", "-1", -1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, atoiSafe(tc.in))
		})
	}
}

// ── nextIndex (#4: round-robin load balancing) ──────────────────────────────────

func TestNextIndex_RoundRobinCycles(t *testing.T) {
	t.Parallel()

	r := &consulRegistry{}

	got := make([]int, 0, 6)
	for range 6 {
		got = append(got, r.nextIndex(3))
	}

	assert.Equal(t, []int{0, 1, 2, 0, 1, 2}, got)
}

func TestNextIndex_SingleOrEmpty(t *testing.T) {
	t.Parallel()

	r := &consulRegistry{}

	assert.Equal(t, 0, r.nextIndex(1))
	assert.Equal(t, 0, r.nextIndex(0))
}

// ── serviceMeta (#5: no caller mutation; serializes scheme + internal endpoint) ──

// Internal-only: all three internal_* keys are written with exact values. The
// "scheme" key mirrors the EXTERNAL scheme only, so it is absent for an
// internal-only service.
func TestServiceMeta_SerializesInternalEndpoint(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{
		Meta: map[string]string{"team": "core"},
		Internal: &Endpoint{
			Address: "svc-a.ns.svc.cluster.local",
			Port:    8080,
			Scheme:  "http",
		},
	})

	_, hasScheme := got["scheme"]
	assert.False(t, hasScheme, "internal-only -> the external scheme mirror must be absent")
	assert.Equal(t, "core", got["team"])
	assert.Equal(t, "svc-a.ns.svc.cluster.local", got["internal_address"])
	assert.Equal(t, "8080", got["internal_port"])
	assert.Equal(t, "http", got["internal_scheme"])
}

// Internal set but Scheme empty: the three internal_* keys are written, "scheme" is not.
func TestServiceMeta_InternalWithoutScheme(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{
		Internal: &Endpoint{Address: "10.0.0.9", Port: 9090, Scheme: "http"},
	})

	_, hasScheme := got["scheme"]
	assert.False(t, hasScheme, "no external scheme -> must not write the scheme key")
	assert.Equal(t, "10.0.0.9", got["internal_address"])
	assert.Equal(t, "9090", got["internal_port"])
	assert.Equal(t, "http", got["internal_scheme"])
}

// Internal port 0 serializes as "0" (no special handling).
func TestServiceMeta_InternalPortZero(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{Internal: &Endpoint{Address: "10.0.0.9", Port: 0, Scheme: "http"}})

	assert.Equal(t, "0", got["internal_port"])
}

// Regression: scheme-only path (no Internal) writes only "scheme" and never
// touches the caller's map.
func TestServiceMeta_SchemeOnlyDoesNotMutateCaller(t *testing.T) {
	t.Parallel()

	orig := map[string]string{"team": "core"}
	got := serviceMeta(Service{Scheme: "https", Meta: orig})

	assert.Equal(t, "https", got["scheme"])
	assert.Equal(t, "core", got["team"])

	_, hasAddr := got["internal_address"]
	assert.False(t, hasAddr, "no Internal -> must not write internal_* keys")

	// The caller's map must be untouched.
	_, ok := orig["scheme"]
	assert.False(t, ok, "serviceMeta must not write into the caller's Meta")
	assert.Equal(t, map[string]string{"team": "core"}, orig)
}

// Caller Meta is never mutated even when internal keys are added (copy-on-write).
func TestServiceMeta_DoesNotMutateCallerWithInternal(t *testing.T) {
	t.Parallel()

	orig := map[string]string{"team": "core"}
	_ = serviceMeta(Service{
		Scheme:   "https",
		Meta:     orig,
		Internal: &Endpoint{Address: "10.0.0.9", Port: 9090, Scheme: "http"},
	})

	assert.Equal(t, map[string]string{"team": "core"}, orig,
		"caller Meta must be unmutated even when internal keys are added")
}

// Generated keys take precedence over pre-existing caller keys of the same name.
func TestServiceMeta_GeneratedKeysTakePrecedence(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{
		Meta:     map[string]string{"scheme": "ftp", "internal_port": "999"},
		External: &Endpoint{Address: "fees.example.net", Port: 443, Scheme: "https"},
		Internal: &Endpoint{Address: "10.0.0.9", Port: 9090, Scheme: "http"},
	})

	assert.Equal(t, "https", got["scheme"], "generated (external) scheme overrides caller value")
	assert.Equal(t, "9090", got["internal_port"], "generated internal_port overrides caller value")
}

// Nothing to add (Scheme empty AND Internal nil): the SAME map reference is
// returned unchanged, not a copy.
func TestServiceMeta_NothingToAddReturnsSameMap(t *testing.T) {
	t.Parallel()

	orig := map[string]string{"team": "core"}
	got := serviceMeta(Service{Meta: orig})

	assert.Equal(t, map[string]string{"team": "core"}, got)
	assert.Equal(t,
		reflect.ValueOf(orig).Pointer(), reflect.ValueOf(got).Pointer(),
		"nothing to add -> must return the caller's map unchanged, not a copy")
}

// Nothing to add and nil Meta: nil stays nil.
func TestServiceMeta_NilMetaStaysNil(t *testing.T) {
	t.Parallel()

	got := serviceMeta(Service{})

	assert.Nil(t, got, "nothing to add and nil Meta -> nil stays nil")
}

// ── backoffDuration (#3: bounded exponential backoff) ───────────────────────────

func TestBackoffDuration(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 100*time.Millisecond, backoffDuration(-1))
	assert.Equal(t, 100*time.Millisecond, backoffDuration(0))
	assert.Equal(t, 200*time.Millisecond, backoffDuration(1))
	assert.Equal(t, 400*time.Millisecond, backoffDuration(2))
	assert.Equal(t, watchBackoffMax, backoffDuration(20), "large attempts cap at watchBackoffMax")
}

// ── sleepCtx (#1/#3: cancellable sleep) ─────────────────────────────────────────

func TestSleepCtx_FullDurationElapses(t *testing.T) {
	t.Parallel()

	assert.True(t, sleepCtx(context.Background(), 10*time.Millisecond))
}

func TestSleepCtx_CancelledReturnsFalse(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.False(t, sleepCtx(ctx, time.Hour), "cancelled ctx must short-circuit the sleep")
}
