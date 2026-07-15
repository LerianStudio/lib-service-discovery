//go:build unit

package libsd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// These tests use t.Setenv, which precludes t.Parallel.

func TestConfigFromEnv_SDPrefix(t *testing.T) {
	t.Setenv("SD_ENABLED", "true")
	t.Setenv("SD_ADDRESS", "consul.example.net:443")
	t.Setenv("SD_ADVERTISE_ADDRESS", "https://fees.example.net")
	t.Setenv("SD_ADVERTISE_PORT", "4002")
	t.Setenv("SD_WORKLOAD", "tenant-a")
	t.Setenv("SD_TLS", "true")
	t.Setenv("SD_TLS_SKIP_VERIFY", "true")
	t.Setenv("SD_TOKEN", "tok-123")

	c := ConfigFromEnv()

	assert.True(t, c.Enabled)
	assert.Equal(t, "consul.example.net:443", c.ConsulAddr)
	assert.Equal(t, "fees.example.net", c.AdvertiseAddr) // host extracted from URL
	assert.Equal(t, "https", c.AdvertiseScheme)
	assert.Equal(t, 4002, c.AdvertisePort)
	assert.Equal(t, "tenant-a", c.Workload)
	assert.True(t, c.TLS)
	assert.True(t, c.TLSSkipVerify)
	assert.Equal(t, "tok-123", c.Token)
}

func TestConfigFromEnv_LegacyFallback(t *testing.T) {
	t.Setenv("SERVICE_DISCOVERY_ENABLED", "true")
	t.Setenv("CONSUL_ADDR", "10.0.0.1:8500")
	t.Setenv("SERVICE_ADVERTISE_ADDR", "fees")
	t.Setenv("SERVICE_ADVERTISE_PORT", "3000")
	t.Setenv("WORKLOAD_ID", "legacy-wl")

	c := ConfigFromEnv()

	assert.True(t, c.Enabled)
	assert.Equal(t, "10.0.0.1:8500", c.ConsulAddr)
	assert.Equal(t, "fees", c.AdvertiseAddr)
	assert.Equal(t, 3000, c.AdvertisePort)
	assert.Equal(t, "legacy-wl", c.Workload)
}

func TestConfigFromEnv_SDTakesPrecedenceOverLegacy(t *testing.T) {
	t.Setenv("SD_ADDRESS", "new:443")
	t.Setenv("CONSUL_ADDR", "old:8500")

	assert.Equal(t, "new:443", ConfigFromEnv().ConsulAddr)
}

// SD_EXTERNAL_ADDRESS/SD_EXTERNAL_PORT are the preferred names; they win over the
// SD_ADVERTISE_* and SERVICE_ADVERTISE_* aliases. Uses t.Setenv, so no t.Parallel.
func TestConfigFromEnv_ExternalAddressPreferredOverAliases(t *testing.T) {
	t.Setenv("SD_EXTERNAL_ADDRESS", "preferred.example.net")
	t.Setenv("SD_ADVERTISE_ADDRESS", "alias.example.net")
	t.Setenv("SERVICE_ADVERTISE_ADDR", "legacy.example.net")
	t.Setenv("SD_EXTERNAL_PORT", "4002")
	t.Setenv("SD_ADVERTISE_PORT", "3000")

	c := ConfigFromEnv()

	assert.Equal(t, "preferred.example.net", c.AdvertiseAddr)
	assert.Equal(t, 4002, c.AdvertisePort)
}

func TestConfigFromEnv_ExternalAddressFallsBackToAliases(t *testing.T) {
	t.Setenv("SD_EXTERNAL_ADDRESS", "")
	t.Setenv("SD_ADVERTISE_ADDRESS", "alias.example.net")
	t.Setenv("SD_EXTERNAL_PORT", "")
	t.Setenv("SD_ADVERTISE_PORT", "3000")

	c := ConfigFromEnv()

	assert.Equal(t, "alias.example.net", c.AdvertiseAddr)
	assert.Equal(t, 3000, c.AdvertisePort)
}

// TestValidate covers the enabled-mode requirements: ConsulAddr is required, but an
// advertise address is NOT — a consumer-only Manager (Enabled, no advertise) is
// valid and only resolves. The advertise requirement now lives in Register. Pure
// (no env), so it runs fully parallel.
func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name:    "disabled is always valid",
			cfg:     Config{Enabled: false},
			wantErr: nil,
		},
		{
			name:    "enabled missing consul addr",
			cfg:     Config{Enabled: true, ConsulAddr: "", AdvertiseAddr: "fees"},
			wantErr: ErrEmptyConsulAddr,
		},
		{
			name:    "enabled external-only is valid",
			cfg:     Config{Enabled: true, ConsulAddr: "localhost:8500", AdvertiseAddr: "fees.example.net"},
			wantErr: nil,
		},
		{
			name:    "enabled internal-only is valid",
			cfg:     Config{Enabled: true, ConsulAddr: "localhost:8500", AdvertiseInternalAddr: "svc.ns.svc.cluster.local"},
			wantErr: nil,
		},
		{
			name:    "enabled consumer-only (no advertise) is valid",
			cfg:     Config{Enabled: true, ConsulAddr: "localhost:8500"},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			assert.NoError(t, err)
		})
	}
}

// TestWithDefaults_SeedTimeout covers the SeedTimeout knob: zero picks up
// defaultSeedTimeout, an explicit value is preserved. Pure (no env), so parallel.
func TestWithDefaults_SeedTimeout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, defaultSeedTimeout, Config{}.withDefaults().SeedTimeout,
		"zero SeedTimeout must default")
	assert.Equal(t, 7*time.Second, Config{SeedTimeout: 7 * time.Second}.withDefaults().SeedTimeout,
		"explicit SeedTimeout must not be overridden")
}

func TestConfigFromEnv_SeedTimeout(t *testing.T) {
	t.Setenv("SD_SEED_TIMEOUT", "1500ms")

	assert.Equal(t, 1500*time.Millisecond, ConfigFromEnv().SeedTimeout)
}

func TestConfigFromEnv_SeedTimeoutDefaultWhenUnset(t *testing.T) {
	t.Setenv("SD_SEED_TIMEOUT", "")

	// ConfigFromEnv leaves zero; withDefaults fills it in New().
	assert.Zero(t, ConfigFromEnv().SeedTimeout)
}

// TestWithDefaults_WatchWaitTime covers the WatchWaitTime knob: zero picks up
// defaultWatchWaitTime (30s), an explicit value is preserved. Pure (no env), parallel.
func TestWithDefaults_WatchWaitTime(t *testing.T) {
	t.Parallel()

	assert.Equal(t, defaultWatchWaitTime, Config{}.withDefaults().WatchWaitTime,
		"zero WatchWaitTime must default")
	assert.Equal(t, 30*time.Second, defaultWatchWaitTime,
		"default watch wait must be short enough to fit under a ~60s reverse-proxy read timeout")
	assert.Equal(t, 45*time.Second, Config{WatchWaitTime: 45 * time.Second}.withDefaults().WatchWaitTime,
		"explicit WatchWaitTime must not be overridden")
}

func TestConfigFromEnv_WatchWaitTime(t *testing.T) {
	t.Setenv("SD_WATCH_WAIT_TIME", "50s")

	assert.Equal(t, 50*time.Second, ConfigFromEnv().WatchWaitTime)
}

func TestConfigFromEnv_DefaultsWhenUnset(t *testing.T) {
	// Isolate from any ambient values.
	t.Setenv("SD_ADDRESS", "")
	t.Setenv("CONSUL_ADDR", "")
	t.Setenv("SD_ENABLED", "")
	t.Setenv("SERVICE_DISCOVERY_ENABLED", "")

	c := ConfigFromEnv()

	assert.False(t, c.Enabled)
	assert.Equal(t, "localhost:8500", c.ConsulAddr)
}

// TestConfigFromEnv_InternalEndpoint covers the cluster-internal advertise
// endpoint. Uses t.Setenv, so neither this test nor its subtests call
// t.Parallel (t.Setenv is incompatible with parallel execution).
func TestConfigFromEnv_InternalEndpoint(t *testing.T) {
	tests := []struct {
		name          string
		env           map[string]string
		wantAddr      string
		wantScheme    string
		wantPort      int
		wantExtAddr   string
		wantExtScheme string
	}{
		{
			name: "internal envs absent -> zero values",
			env: map[string]string{
				"SD_INTERNAL_ADDRESS": "",
				"SD_INTERNAL_SCHEME":  "",
				"SD_INTERNAL_PORT":    "",
			},
			wantAddr:   "",
			wantScheme: "",
			wantPort:   0,
		},
		{
			name: "bare hostname with explicit scheme and port",
			env: map[string]string{
				"SD_INTERNAL_ADDRESS": "svc.ns.svc.cluster.local",
				"SD_INTERNAL_SCHEME":  "http",
				"SD_INTERNAL_PORT":    "8080",
			},
			wantAddr:   "svc.ns.svc.cluster.local",
			wantScheme: "http",
			wantPort:   8080,
		},
		{
			name: "url scheme extracted and host stripped",
			env: map[string]string{
				"SD_INTERNAL_ADDRESS": "https://svc.ns.svc.cluster.local",
			},
			wantAddr:   "svc.ns.svc.cluster.local",
			wantScheme: "https",
			wantPort:   0,
		},
		{
			name: "explicit scheme wins over url scheme",
			env: map[string]string{
				"SD_INTERNAL_SCHEME":  "http",
				"SD_INTERNAL_ADDRESS": "https://svc.ns.svc.cluster.local",
			},
			wantAddr:   "svc.ns.svc.cluster.local",
			wantScheme: "http",
			wantPort:   0,
		},
		{
			name: "external and internal pairs both parse (regression)",
			env: map[string]string{
				"SD_ADVERTISE_ADDRESS": "https://fees.example.net",
				"SD_INTERNAL_ADDRESS":  "https://fees.ns.svc.cluster.local",
			},
			wantAddr:      "fees.ns.svc.cluster.local",
			wantScheme:    "https",
			wantExtAddr:   "fees.example.net",
			wantExtScheme: "https",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			c := ConfigFromEnv()

			assert.Equal(t, tt.wantAddr, c.AdvertiseInternalAddr)
			assert.Equal(t, tt.wantScheme, c.AdvertiseInternalScheme)
			assert.Equal(t, tt.wantPort, c.AdvertiseInternalPort)

			if tt.wantExtAddr != "" {
				assert.Equal(t, tt.wantExtAddr, c.AdvertiseAddr)
				assert.Equal(t, tt.wantExtScheme, c.AdvertiseScheme)
			}
		})
	}
}

// TestConfigFromEnv_PreferView covers SD_PREFER_VIEW parsing. Uses t.Setenv, so
// neither this test nor its subtests call t.Parallel.
func TestConfigFromEnv_PreferView(t *testing.T) {
	tests := []struct {
		name string
		set  bool
		val  string
		want EndpointView
	}{
		{name: "unset defaults to external", set: false, want: External},
		{name: "external", set: true, val: "external", want: External},
		{name: "internal", set: true, val: "internal", want: Internal},
		{name: "uppercase INTERNAL", set: true, val: "INTERNAL", want: Internal},
		{name: "garbage defaults to external", set: true, val: "garbage", want: External},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// t.Setenv precludes t.Parallel; always set to isolate ambient values.
			if tt.set {
				t.Setenv("SD_PREFER_VIEW", tt.val)
			} else {
				t.Setenv("SD_PREFER_VIEW", "")
			}

			c := ConfigFromEnv()

			assert.Equal(t, tt.want, c.PreferView)
		})
	}
}

// TestParsePreferView is a pure unit test of the view normalizer and runs fully
// parallel (no env access).
func TestParsePreferView(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want EndpointView
	}{
		{name: "empty defaults to external", raw: "", want: External},
		{name: "internal", raw: "internal", want: Internal},
		{name: "internal trimmed and upper", raw: "  INTERNAL  ", want: Internal},
		{name: "external", raw: "external", want: External},
		{name: "garbage defaults to external", raw: "xyz", want: External},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, parsePreferView(tt.raw))
		})
	}
}

// TestSplitSchemeHost is a pure unit test of the scheme/host splitter and runs
// fully parallel (no env access).
func TestSplitSchemeHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		addr       string
		wantScheme string
		wantHost   string
		wantPort   string
	}{
		{name: "empty", addr: "", wantScheme: "", wantHost: "", wantPort: ""},
		{name: "bare hostname", addr: "fees.example.net", wantScheme: "", wantHost: "fees.example.net", wantPort: ""},
		{name: "https url", addr: "https://fees.example.net", wantScheme: "https", wantHost: "fees.example.net", wantPort: ""},
		{name: "https url with port", addr: "https://svc:8443", wantScheme: "https", wantHost: "svc", wantPort: "8443"},
		{name: "http cluster dns", addr: "http://svc.ns.svc.cluster.local", wantScheme: "http", wantHost: "svc.ns.svc.cluster.local", wantPort: ""},
		{name: "malformed returns addr unchanged", addr: "://bad", wantScheme: "", wantHost: "://bad", wantPort: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scheme, host, port := splitSchemeHost(tt.addr)

			assert.Equal(t, tt.wantScheme, scheme)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantPort, port)
		})
	}
}

// TestParseAdvertiseAddrs_URLPortPreserved covers #5: a URL that carries a port
// preserves it when no explicit port is configured, and an explicit port always
// wins over the URL port. Pure (no env), so it runs fully parallel.
func TestParseAdvertiseAddrs_URLPortPreserved(t *testing.T) {
	t.Parallel()

	t.Run("external url port preserved when AdvertisePort unset", func(t *testing.T) {
		t.Parallel()

		c := Config{AdvertiseAddr: "https://svc:8443"}.parseAdvertiseAddrs()

		assert.Equal(t, "svc", c.AdvertiseAddr)
		assert.Equal(t, "https", c.AdvertiseScheme)
		assert.Equal(t, 8443, c.AdvertisePort, "URL port must be preserved when no explicit port is set")
	})

	t.Run("explicit external port wins over url port", func(t *testing.T) {
		t.Parallel()

		c := Config{AdvertiseAddr: "https://svc:8443", AdvertisePort: 9000}.parseAdvertiseAddrs()

		assert.Equal(t, 9000, c.AdvertisePort, "explicit AdvertisePort must win over the URL port")
	})

	t.Run("internal url port preserved when AdvertiseInternalPort unset", func(t *testing.T) {
		t.Parallel()

		c := Config{AdvertiseInternalAddr: "http://svc.ns.svc.cluster.local:9090"}.parseAdvertiseAddrs()

		assert.Equal(t, "svc.ns.svc.cluster.local", c.AdvertiseInternalAddr)
		assert.Equal(t, "http", c.AdvertiseInternalScheme)
		assert.Equal(t, 9090, c.AdvertiseInternalPort, "internal URL port must be preserved when no explicit port is set")
	})

	t.Run("explicit internal port wins over url port", func(t *testing.T) {
		t.Parallel()

		c := Config{AdvertiseInternalAddr: "http://svc:9090", AdvertiseInternalPort: 1234}.parseAdvertiseAddrs()

		assert.Equal(t, 1234, c.AdvertiseInternalPort, "explicit AdvertiseInternalPort must win over the URL port")
	})
}

// TestConfigFromEnv_URLPortWithoutExplicitPort covers #5 at the env layer:
// SD_ADVERTISE_ADDRESS=https://svc:8443 with no SD_ADVERTISE_PORT preserves 8443.
func TestConfigFromEnv_URLPortWithoutExplicitPort(t *testing.T) {
	t.Setenv("SD_ADVERTISE_ADDRESS", "https://svc:8443")
	t.Setenv("SD_ADVERTISE_PORT", "")
	t.Setenv("SD_EXTERNAL_PORT", "")

	c := ConfigFromEnv()

	assert.Equal(t, "svc", c.AdvertiseAddr)
	assert.Equal(t, "https", c.AdvertiseScheme)
	assert.Equal(t, 8443, c.AdvertisePort)
}
