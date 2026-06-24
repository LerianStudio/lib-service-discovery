package libsd

import (
	"net/url"
	"os"
	"strconv"

	"github.com/LerianStudio/lib-observability/log"
)

// Config holds the configuration for the Manager.
// Populate it manually or call ConfigFromEnv to load from environment variables.
type Config struct {
	// Enabled controls whether Consul-backed discovery is active.
	// When false all operations are no-ops and Resolve returns the fallback.
	Enabled bool

	// ConsulAddr is the address of the Consul agent (host:port).
	// Defaults to "localhost:8500".
	ConsulAddr string

	// AdvertiseAddr is the address this service advertises to Consul.
	// Required when Enabled is true. Accepts either a plain hostname
	// ("fees.dev.example.net") or a full URL ("https://fees.dev.example.net").
	// When a full URL is provided, the scheme is extracted into AdvertiseScheme
	// automatically by ConfigFromEnv and withDefaults.
	AdvertiseAddr string

	// AdvertiseScheme is the URL scheme ("https", "http") stored in Consul
	// Meta["scheme"] when this service registers. Set automatically when
	// AdvertiseAddr contains a scheme prefix.
	AdvertiseScheme string

	// AdvertisePort overrides the port reported to Consul.
	// When zero the port passed to Register is used as-is.
	AdvertisePort int

	// Workload scopes this Manager to a logical deployment group (e.g. "tenant-a").
	// When set, Register automatically tags services with "workload=<id>" and
	// Resolve filters Consul results to instances carrying that same tag.
	// Leave empty to disable workload filtering (all healthy instances are candidates).
	Workload string

	// TLS enables HTTPS to the discovery server. Maps to SD_TLS.
	TLS bool

	// TLSSkipVerify disables server certificate verification (dev/self-signed).
	// Maps to SD_TLS_SKIP_VERIFY.
	TLSSkipVerify bool

	// Token is the ACL token sent to the discovery server. Maps to SD_TOKEN.
	Token string

	// Logger receives structured log output from the Manager and registry.
	// Defaults to log.NewNop() when nil.
	Logger log.Logger
}

// ConfigFromEnv returns a Config populated from environment variables.
//
// Canonical, backend-agnostic SD_* names (legacy names accepted as fallback):
//
//	SD_ENABLED          — "true" to enable               (legacy: SERVICE_DISCOVERY_ENABLED)
//	SD_ADDRESS          — discovery server addr host:port (legacy: CONSUL_ADDR; default "localhost:8500")
//	SD_ADVERTISE_ADDRESS— hostname or full URL this instance advertises (legacy: SERVICE_ADVERTISE_ADDR)
//	SD_ADVERTISE_PORT   — port override, 0 = use Register port (legacy: SERVICE_ADVERTISE_PORT)
//	SD_WORKLOAD         — workload scope for tag filtering (legacy: WORKLOAD_ID)
//	SD_TLS              — "true" for HTTPS to the server
//	SD_TLS_SKIP_VERIFY  — "true" to skip server cert verification
//	SD_TOKEN            — ACL token
func ConfigFromEnv() Config {
	c := Config{
		Enabled:       anyEnvTrue("SD_ENABLED", "SERVICE_DISCOVERY_ENABLED"),
		ConsulAddr:    firstEnv("localhost:8500", "SD_ADDRESS", "CONSUL_ADDR"),
		AdvertiseAddr: firstEnv("", "SD_ADVERTISE_ADDRESS", "SERVICE_ADVERTISE_ADDR"),
		AdvertisePort: firstEnvInt(0, "SD_ADVERTISE_PORT", "SERVICE_ADVERTISE_PORT"),
		Workload:      firstEnv("", "SD_WORKLOAD", "WORKLOAD_ID"),
		TLS:           os.Getenv("SD_TLS") == "true",
		TLSSkipVerify: os.Getenv("SD_TLS_SKIP_VERIFY") == "true",
		Token:         os.Getenv("SD_TOKEN"),
	}

	c = c.parseAdvertiseAddr()

	return c
}

// Validate returns an error when the Config is inconsistent.
// When Enabled is true, both ConsulAddr and AdvertiseAddr are required.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.ConsulAddr == "" {
		return ErrEmptyConsulAddr
	}

	if c.AdvertiseAddr == "" {
		return ErrEmptyAdvertiseAddr
	}

	return nil
}

func (c Config) withDefaults() Config {
	if c.Logger == nil {
		c.Logger = log.NewNop()
	}

	if c.ConsulAddr == "" {
		c.ConsulAddr = "localhost:8500"
	}

	return c.parseAdvertiseAddr()
}

// parseAdvertiseAddr extracts the scheme from AdvertiseAddr when it contains "://"
// so callers can use "https://fees.example.net" or plain "fees.example.net".
func (c Config) parseAdvertiseAddr() Config {
	if c.AdvertiseAddr == "" {
		return c
	}

	u, err := url.Parse(c.AdvertiseAddr)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return c
	}

	if c.AdvertiseScheme == "" {
		c.AdvertiseScheme = u.Scheme
	}

	c.AdvertiseAddr = u.Hostname()

	return c
}

// firstEnv returns the first non-empty value among keys (precedence order),
// or fallback when none is set. Used to prefer SD_* over legacy names.
func firstEnv(fallback string, keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}

	return fallback
}

// firstEnvInt is firstEnv parsed as an int.
func firstEnvInt(fallback int, keys ...string) int {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
	}

	return fallback
}

// anyEnvTrue reports whether any of keys is set to the string "true".
func anyEnvTrue(keys ...string) bool {
	for _, k := range keys {
		if os.Getenv(k) == "true" {
			return true
		}
	}

	return false
}
