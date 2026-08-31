package libsd

import (
	"context"
	"reflect"

	"github.com/LerianStudio/lib-service-discovery/v2/obs"
)

// Logger is the structured-logging contract that lib-service-discovery accepts
// from consumers. It is an alias for obs.Logger, declared in a package that
// imports nothing but the stdlib, so no version of lib-observability — or of
// any other module — is nominally bound into this library's public API.
//
// Any logger satisfies it: a lib-observability v4 log.Logger directly, a
// lib-commons obs.Logger directly, and a type declared in a consumer package
// that has never imported an observability library at all. See the obs package
// documentation and MIGRATION-v3.md.
type Logger = obs.Logger

// Log severity levels, re-exported from obs so a consumer that only imports
// libsd can name them. Lower values are MORE severe; the scale is identical to
// lib-observability's.
const (
	LevelError = obs.LevelError
	LevelWarn  = obs.LevelWarn
	LevelInfo  = obs.LevelInfo
	LevelDebug = obs.LevelDebug
)

// nopLogger is the silent logger used when no logger is supplied. It is
// declared here rather than borrowed from lib-observability so the default
// path — by far the most common one — depends on nothing.
type nopLogger struct{}

func (nopLogger) Log(context.Context, int, string, ...any) {}

func (nopLogger) Enabled(int) bool { return false }

func (nopLogger) Sync(context.Context) error { return nil }

// isNilLogger reports whether l is nil, including a typed nil (e.g. a nil
// *slog.Logger stored in the interface), which l == nil alone cannot catch and
// which would panic on the first log call.
func isNilLogger(l Logger) bool {
	if l == nil {
		return true
	}

	v := reflect.ValueOf(l)

	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Func, reflect.Chan, reflect.Slice, reflect.Interface:
		return v.IsNil()
	default:
		return false
	}
}

// orNop returns l, or a silent no-op logger when l is nil or a typed nil, so
// every internal call site can log unconditionally without a nil check.
func orNop(l Logger) Logger {
	if isNilLogger(l) {
		return nopLogger{}
	}

	return l
}
