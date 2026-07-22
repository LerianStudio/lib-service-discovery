//go:build unit || integration

package libsd

import "log/slog"

// nopLogger returns a silent, slog-compatible libsd.Logger for tests that do
// not assert on emitted messages. Shared by the unit and integration suites.
func nopLogger() Logger { return slog.New(slog.DiscardHandler) }
