//go:build unit || integration

package libsd

// discardLogger returns a silent libsd.Logger for tests that do not assert on
// emitted messages. Shared by the unit and integration suites.
func discardLogger() Logger { return nopLogger{} }
