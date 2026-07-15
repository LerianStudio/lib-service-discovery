//go:build unit

package libsd

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── GAP-5: AllowStale defaults to true (*bool override) ─────────────────────────

func TestWithDefaults_AllowStaleDefaultsTrue(t *testing.T) {
	t.Parallel()

	c := Config{}.withDefaults()
	require.NotNil(t, c.AllowStale, "AllowStale must be non-nil after withDefaults")
	assert.True(t, *c.AllowStale, "AllowStale must default to true (stale reads)")
}

func TestWithDefaults_AllowStaleExplicitFalsePreserved(t *testing.T) {
	t.Parallel()

	f := false
	c := Config{AllowStale: &f}.withDefaults()
	require.NotNil(t, c.AllowStale)
	assert.False(t, *c.AllowStale, "an explicit false override must be preserved")
}

func TestWithDefaults_AllowStaleExplicitTruePreserved(t *testing.T) {
	t.Parallel()

	tr := true
	c := Config{AllowStale: &tr}.withDefaults()
	require.NotNil(t, c.AllowStale)
	assert.True(t, *c.AllowStale)
}

func TestConfigFromEnv_AllowStaleUnsetIsNil(t *testing.T) {
	t.Setenv("SD_ALLOW_STALE", "")

	assert.Nil(t, ConfigFromEnv().AllowStale, "unset SD_ALLOW_STALE leaves AllowStale nil (withDefaults→true)")
}

func TestConfigFromEnv_AllowStaleFalse(t *testing.T) {
	t.Setenv("SD_ALLOW_STALE", "false")

	c := ConfigFromEnv()
	require.NotNil(t, c.AllowStale)
	assert.False(t, *c.AllowStale)
}

func TestConfigFromEnv_AllowStaleTrue(t *testing.T) {
	t.Setenv("SD_ALLOW_STALE", "true")

	c := ConfigFromEnv()
	require.NotNil(t, c.AllowStale)
	assert.True(t, *c.AllowStale)
}

// A Manager built via New with the default (unset) AllowStale resolves to stale
// reads in the Consul backend's query options.
func TestNew_DefaultAllowStaleTrueInQueryOpts(t *testing.T) {
	t.Parallel()

	reg, err := newConsulRegistry(Config{ConsulAddr: "localhost:8500"}.withDefaults(), log.NewNop())
	require.NoError(t, err)

	cr, ok := reg.(*consulRegistry)
	require.True(t, ok)
	assert.True(t, cr.queryOpts(context.Background()).AllowStale,
		"default AllowStale must produce stale reads")
}
