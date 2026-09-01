//go:build unit

package libsd

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingLogger is a libsd.Logger declared with stdlib types only — it names
// nothing from lib-observability. That it compiles at all is part of what these
// tests assert: the contract is satisfiable from a foreign package.
type recordingLogger struct {
	calls  int
	level  int
	msg    string
	fields []any
	synced bool
}

func (r *recordingLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	r.calls++
	r.level, r.msg, r.fields = level, msg, fields
}

func (r *recordingLogger) Enabled(level int) bool { return level <= LevelInfo }

func (r *recordingLogger) Sync(context.Context) error {
	r.synced = true

	return nil
}

// ── The default when no logger is supplied ───────────────────────────────────

func TestOrNop_NilYieldsSilentLogger(t *testing.T) {
	t.Parallel()

	got := orNop(nil)
	require.NotNil(t, got)

	assert.NotPanics(t, func() {
		got.Log(context.Background(), LevelInfo, "silent", log.String("k", "v"))
	})
	assert.False(t, got.Enabled(LevelError), "the no-op logger reports nothing enabled")
	assert.NoError(t, got.Sync(context.Background()))
}

// A typed-nil pointer stored in the Logger interface is not caught by a plain
// == nil check; without a kind-aware guard it would be used and panic on the
// first call.
func TestOrNop_TypedNilYieldsSilentLogger(t *testing.T) {
	t.Parallel()

	var typedNil *recordingLogger

	got := orNop(typedNil)
	require.NotNil(t, got)

	assert.NotPanics(t, func() {
		got.Log(context.Background(), LevelError, "probe")
	})
}

func TestOrNop_PassesLoggerThroughUnwrapped(t *testing.T) {
	t.Parallel()

	rec := &recordingLogger{}

	// No adapter: the consumer's own value is what the library holds and calls.
	assert.Same(t, rec, orNop(rec))
}

func TestIsNilLogger(t *testing.T) {
	t.Parallel()

	var typedNilPtr *recordingLogger

	var typedNilSlice sliceLogger

	cases := []struct {
		name string
		in   Logger
		want bool
	}{
		{"untyped nil", nil, true},
		{"typed nil pointer", typedNilPtr, true},
		{"typed nil of a non-pointer kind", typedNilSlice, true},
		{"non-nil pointer", &recordingLogger{}, false},
		{"non-pointer value", nopLogger{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, isNilLogger(tc.in))
		})
	}
}

// sliceLogger is a Logger whose zero value is a nil slice: a typed nil that is
// not a pointer, which the guard must still catch.
type sliceLogger []string

func (sliceLogger) Log(context.Context, int, string, ...any) {}

func (sliceLogger) Enabled(int) bool { return false }

func (sliceLogger) Sync(context.Context) error { return nil }

// ── The levels the library emits ─────────────────────────────────────────────

// The libsd level constants must stay numerically identical to
// lib-observability's, or every severity the library emits is silently
// mislabelled at the consumer.
func TestLevels_MatchLibObservabilityScale(t *testing.T) {
	t.Parallel()

	assert.Equal(t, log.LevelError, LevelError)
	assert.Equal(t, log.LevelWarn, LevelWarn)
	assert.Equal(t, log.LevelInfo, LevelInfo)
	assert.Equal(t, log.LevelDebug, LevelDebug)
}

// ── Loggers from elsewhere are accepted directly ─────────────────────────────

// A lib-observability v4 logger satisfies libsd.Logger with no adapter. This is
// the property that lets the fleet move majors independently of this library.
func TestLibObservabilityLoggerSatisfiesContract(t *testing.T) {
	t.Parallel()

	var l Logger = log.NewNop()
	require.NotNil(t, l)

	assert.NotPanics(t, func() {
		l.Log(context.Background(), LevelInfo, "accepted directly")
	})
}

// ── Wiring ───────────────────────────────────────────────────────────────────

func TestWithLogger_TypedNilKeepsConfiguredLogger(t *testing.T) {
	t.Parallel()

	rec := &recordingLogger{}
	m := &Manager{logger: rec}

	var typedNil *recordingLogger

	WithLogger(typedNil)(m)

	m.logger.Log(context.Background(), LevelInfo, "still recording")
	assert.Equal(t, "still recording", rec.msg, "typed-nil WithLogger must not replace the configured logger")
}

func TestNew_NoLoggerConfiguredIsSilentAndSafe(t *testing.T) {
	t.Parallel()

	m, err := New(Config{Enabled: false})
	require.NoError(t, err)
	require.NotNil(t, m.logger)

	// Every internal call site logs unconditionally; with no logger configured
	// that must be silent rather than a nil dereference.
	assert.NotPanics(t, func() {
		m.logger.Log(context.Background(), LevelWarn, "no logger configured", log.Err(assert.AnError))
	})
}

func TestNew_ConfigLoggerReceivesLibraryOutput(t *testing.T) {
	t.Parallel()

	rec := &recordingLogger{}

	m, err := New(Config{Enabled: false, Logger: rec})
	require.NoError(t, err)
	assert.Same(t, rec, m.logger, "the configured logger must be held as-is, with no adapter")

	m.logger.Log(context.Background(), LevelWarn, "hello", log.String("service", "svc-a"))
	assert.Equal(t, LevelWarn, rec.level)
	assert.Equal(t, "hello", rec.msg)
	assert.Len(t, rec.fields, 1)
}
