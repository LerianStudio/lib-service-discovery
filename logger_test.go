//go:build unit

package libsd

import (
	"context"
	"log/slog"
	"testing"

	"github.com/LerianStudio/lib-observability/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingLogger is a libsd.Logger that captures the level, message and args
// of each call, so the adapter's level mapping and field translation can be
// asserted.
type recordingLogger struct {
	level string
	msg   string
	args  []any
}

func (r *recordingLogger) InfoContext(_ context.Context, msg string, args ...any) {
	r.level, r.msg, r.args = "info", msg, args
}

func (r *recordingLogger) WarnContext(_ context.Context, msg string, args ...any) {
	r.level, r.msg, r.args = "warn", msg, args
}

func (r *recordingLogger) ErrorContext(_ context.Context, msg string, args ...any) {
	r.level, r.msg, r.args = "error", msg, args
}

func (r *recordingLogger) DebugContext(_ context.Context, msg string, args ...any) {
	r.level, r.msg, r.args = "debug", msg, args
}

func TestToObsLogger_NilYieldsNop(t *testing.T) {
	t.Parallel()

	// A nil public logger must produce a usable, silent internal logger.
	got := toObsLogger(nil)
	require.NotNil(t, got)
	assert.NotPanics(t, func() {
		got.Log(context.Background(), log.LevelInfo, "silent")
	})
}

func TestObsLoggerAdapter_LevelMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		level log.Level
		want  string
	}{
		{log.LevelError, "error"},
		{log.LevelWarn, "warn"},
		{log.LevelInfo, "info"},
		{log.LevelDebug, "debug"},
		{log.LevelUnknown, "info"}, // unknown levels default to info
	}

	for _, tc := range cases {
		rec := &recordingLogger{}
		toObsLogger(rec).Log(context.Background(), tc.level, "msg")
		assert.Equal(t, tc.want, rec.level, "level %v", tc.level)
	}
}

func TestObsLoggerAdapter_FieldsBecomeSlogArgs(t *testing.T) {
	t.Parallel()

	rec := &recordingLogger{}
	toObsLogger(rec).Log(context.Background(), log.LevelInfo, "resolved",
		log.String("service", "svc-a"), log.Int("count", 3))

	assert.Equal(t, "resolved", rec.msg)
	assert.Equal(t, []any{"service", "svc-a", "count", 3}, rec.args)
}

func TestObsLoggerAdapter_WithAndWithGroup(t *testing.T) {
	t.Parallel()

	rec := &recordingLogger{}

	l := toObsLogger(rec).
		With(log.String("base", "b")).
		WithGroup("grp")
	l.Log(context.Background(), log.LevelWarn, "m", log.String("k", "v"))

	// Accumulated With attrs come first; grouped keys are dotted.
	assert.Equal(t, []any{"base", "b", "grp.k", "v"}, rec.args)
	assert.True(t, l.Enabled(log.LevelDebug))
	assert.NoError(t, l.Sync(context.Background()))
}

// A typed-nil *slog.Logger stored in the Logger interface is not caught by a
// plain == nil check; without a kind-aware guard the adapter wraps it and the
// first log call panics inside slog. Both entry points must treat it as nil.
func TestToObsLogger_TypedNilYieldsNop(t *testing.T) {
	t.Parallel()

	var sl *slog.Logger

	l := toObsLogger(sl)
	require.NotNil(t, l)

	assert.NotPanics(t, func() {
		l.Log(context.Background(), log.LevelInfo, "probe")
	})
}

func TestWithLogger_TypedNilKeepsConfiguredLogger(t *testing.T) {
	t.Parallel()

	rec := &recordingLogger{}
	m := &Manager{logger: toObsLogger(rec)}

	var sl *slog.Logger
	WithLogger(sl)(m)

	m.logger.Log(context.Background(), log.LevelInfo, "still recording")
	assert.Equal(t, "still recording", rec.msg, "typed-nil WithLogger must not replace the configured logger")
}
