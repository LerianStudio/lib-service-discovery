//go:build unit

package libsd

import (
	"testing"

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
