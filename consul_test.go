//go:build unit

package libsd

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
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

// ── metaWithScheme (#5: no caller mutation) ─────────────────────────────────────

func TestMetaWithScheme_CopiesWithoutMutatingCaller(t *testing.T) {
	t.Parallel()

	orig := map[string]string{"team": "core"}
	got := metaWithScheme(Service{Scheme: "https", Meta: orig})

	assert.Equal(t, "https", got["scheme"])
	assert.Equal(t, "core", got["team"])

	// The caller's map must be untouched.
	_, ok := orig["scheme"]
	assert.False(t, ok, "metaWithScheme must not write into the caller's Meta")
	assert.Equal(t, map[string]string{"team": "core"}, orig)
}

func TestMetaWithScheme_NoSchemeReturnsOriginal(t *testing.T) {
	t.Parallel()

	orig := map[string]string{"team": "core"}
	got := metaWithScheme(Service{Meta: orig})

	assert.Equal(t, map[string]string{"team": "core"}, got)
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
