//go:build unit

package libsd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Task 1: tuned HTTP transport (connection timeouts without killing watches) ──

// The "fast" client (Resolve/Register/Deregister/heartbeat) carries a
// ResponseHeaderTimeout because those are short request/response exchanges. The
// dial and TLS-handshake timeouts bound connection establishment against a dead
// single-node Consul.
func TestNewTunedConfig_FastClientHasAllTimeouts(t *testing.T) {
	t.Parallel()

	c := Config{
		ConsulAddr:            "consul.example.net:8500",
		DialTimeout:           3 * time.Second,
		TLSHandshakeTimeout:   4 * time.Second,
		ResponseHeaderTimeout: 7 * time.Second,
	}

	cfg := newTunedConfig(c, true)

	require.NotNil(t, cfg.Transport)
	assert.Equal(t, "consul.example.net:8500", cfg.Address)
	assert.NotNil(t, cfg.Transport.DialContext, "dial context must be set")
	assert.Equal(t, 4*time.Second, cfg.Transport.TLSHandshakeTimeout)
	assert.Equal(t, 7*time.Second, cfg.Transport.ResponseHeaderTimeout)
}

// The "watch" client MUST NOT carry a ResponseHeaderTimeout: a Consul blocking
// query withholds response headers until the catalog index advances (up to
// watchWaitTime), so a response-header deadline would abort healthy long-polls.
func TestNewTunedConfig_WatchClientOmitsResponseHeaderTimeout(t *testing.T) {
	t.Parallel()

	c := Config{
		ConsulAddr:            "consul.example.net:8500",
		DialTimeout:           3 * time.Second,
		TLSHandshakeTimeout:   4 * time.Second,
		ResponseHeaderTimeout: 7 * time.Second,
	}

	cfg := newTunedConfig(c, false)

	require.NotNil(t, cfg.Transport)
	assert.NotNil(t, cfg.Transport.DialContext, "dial context must still be set on watch client")
	assert.Equal(t, 4*time.Second, cfg.Transport.TLSHandshakeTimeout)
	assert.Zero(t, cfg.Transport.ResponseHeaderTimeout,
		"watch client must have no response-header deadline so long-polls survive")
}

// TLS options must still flow through the tuned config: leaving Transport.TLSClientConfig
// nil lets consul's NewHttpClient apply cfg.TLSConfig (including InsecureSkipVerify).
func TestNewTunedConfig_PreservesTLSOptions(t *testing.T) {
	t.Parallel()

	cfg := newTunedConfig(Config{
		ConsulAddr:    "consul.example.net:8500",
		TLS:           true,
		TLSSkipVerify: true,
		Token:         "tok-xyz",
	}, true)

	assert.Equal(t, "https", cfg.Scheme)
	assert.True(t, cfg.TLSConfig.InsecureSkipVerify)
	assert.Equal(t, "tok-xyz", cfg.Token)
	assert.Nil(t, cfg.Transport.TLSClientConfig,
		"TLSClientConfig must stay nil so consul applies cfg.TLSConfig to the transport")
}

// ── Task 2: safe TTL floor ──────────────────────────────────────────────────

func TestTTLWithDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "empty applies safe default", in: "", want: "30s"},
		{name: "whitespace applies safe default", in: "   ", want: "30s"},
		{name: "at floor kept", in: "15s", want: "15s"},
		{name: "above floor kept", in: "30s", want: "30s"},
		{name: "minutes kept", in: "1m", want: "1m0s"},
		{name: "below floor raised to floor", in: "5s", want: "15s"},
		{name: "just below floor raised to floor", in: "14s", want: "15s"},
		{name: "trimmed then parsed", in: "  20s  ", want: "20s"},
		{name: "unparseable errors", in: "not-a-duration", wantErr: true},
		{name: "zero errors", in: "0s", wantErr: false, want: "15s"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ttlWithDefaults(tt.in)
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrInvalidTTL)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Register must clamp a below-floor TTL before it reaches the registry, so a
// GC pause or a brief blip never triggers a false deregistration.
func TestRegister_ClampsBelowFloorTTL(t *testing.T) {
	t.Parallel()

	var registered Service

	m := enabledManager(t, &captureRegistry{onRegister: func(svc Service) { registered = svc }})

	err := m.Register(context.Background(), Service{
		Name:        "svc-a",
		Port:        8081,
		HealthCheck: &HealthCheck{TTL: "5s"},
	})
	require.NoError(t, err)
	require.NotNil(t, registered.HealthCheck)
	assert.Equal(t, "15s", registered.HealthCheck.TTL)
}

// An unparseable TTL is a hard configuration error surfaced from Register.
func TestRegister_RejectsInvalidTTL(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})

	err := m.Register(context.Background(), Service{
		Name:        "svc-a",
		Port:        8081,
		HealthCheck: &HealthCheck{TTL: "banana"},
	})
	assert.ErrorIs(t, err, ErrInvalidTTL)
}

// Register must NOT mutate the caller's HealthCheck when clamping the TTL.
func TestRegister_DoesNotMutateCallerHealthCheckTTL(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})

	hc := &HealthCheck{TTL: "5s"}
	err := m.Register(context.Background(), Service{Name: "svc-a", Port: 8081, HealthCheck: hc})
	require.NoError(t, err)
	assert.Equal(t, "5s", hc.TTL, "caller's HealthCheck must be left untouched")
}

// ── Task 3: AllowStale opt-in ────────────────────────────────────────────────

func TestQueryOpts_AllowStaleOptIn(t *testing.T) {
	t.Parallel()

	stale := &consulRegistry{allowStale: true}
	assert.True(t, stale.queryOpts(context.Background()).AllowStale)

	strong := &consulRegistry{allowStale: false}
	assert.False(t, strong.queryOpts(context.Background()).AllowStale)
}

// queryOpts is nil-receiver safe like the rest of the library.
func TestQueryOpts_NilReceiverSafe(t *testing.T) {
	t.Parallel()

	var r *consulRegistry
	opts := r.queryOpts(context.Background())
	require.NotNil(t, opts)
	assert.False(t, opts.AllowStale)
}

// ── Config wiring for the new knobs ──────────────────────────────────────────

func TestWithDefaults_HardeningDefaults(t *testing.T) {
	t.Parallel()

	c := Config{}.withDefaults()

	assert.Equal(t, defaultDialTimeout, c.DialTimeout)
	assert.Equal(t, defaultTLSHandshakeTimeout, c.TLSHandshakeTimeout)
	assert.Equal(t, defaultResponseHeaderTimeout, c.ResponseHeaderTimeout)
	require.NotNil(t, c.AllowStale, "AllowStale must be non-nil after withDefaults")
	assert.True(t, *c.AllowStale, "AllowStale now defaults to true (stale reads, available during a leader blip)")
}

func TestWithDefaults_DoesNotOverrideExplicitTimeouts(t *testing.T) {
	t.Parallel()

	c := Config{
		DialTimeout:           1 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ResponseHeaderTimeout: 3 * time.Second,
	}.withDefaults()

	assert.Equal(t, 1*time.Second, c.DialTimeout)
	assert.Equal(t, 2*time.Second, c.TLSHandshakeTimeout)
	assert.Equal(t, 3*time.Second, c.ResponseHeaderTimeout)
}

func TestConfigFromEnv_HardeningVars(t *testing.T) {
	t.Setenv("SD_DIAL_TIMEOUT", "2s")
	t.Setenv("SD_TLS_HANDSHAKE_TIMEOUT", "3s")
	t.Setenv("SD_RESPONSE_HEADER_TIMEOUT", "12s")
	t.Setenv("SD_ALLOW_STALE", "true")

	c := ConfigFromEnv()

	assert.Equal(t, 2*time.Second, c.DialTimeout)
	assert.Equal(t, 3*time.Second, c.TLSHandshakeTimeout)
	assert.Equal(t, 12*time.Second, c.ResponseHeaderTimeout)
	require.NotNil(t, c.AllowStale)
	assert.True(t, *c.AllowStale)
}

func TestConfigFromEnv_HardeningVarsDefaultWhenUnset(t *testing.T) {
	t.Setenv("SD_DIAL_TIMEOUT", "")
	t.Setenv("SD_TLS_HANDSHAKE_TIMEOUT", "")
	t.Setenv("SD_RESPONSE_HEADER_TIMEOUT", "")
	t.Setenv("SD_ALLOW_STALE", "")

	c := ConfigFromEnv()

	// ConfigFromEnv leaves zero values; withDefaults fills them in New().
	assert.Zero(t, c.DialTimeout)
	assert.Zero(t, c.TLSHandshakeTimeout)
	assert.Zero(t, c.ResponseHeaderTimeout)
	assert.Nil(t, c.AllowStale, "unset SD_ALLOW_STALE leaves AllowStale nil; withDefaults fills it (→true) in New()")
}
