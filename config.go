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

	// Logger receives structured log output from the Manager and registry.
	// Defaults to log.NewNop() when nil.
	Logger log.Logger
}

// ConfigFromEnv returns a Config populated from environment variables.
//
//	SERVICE_DISCOVERY_ENABLED — "true" to enable (default: disabled)
//	CONSUL_ADDR               — Consul agent address (default: "localhost:8500")
//	SERVICE_ADVERTISE_ADDR    — hostname or full URL ("https://fees.example.net") this instance advertises
//	SERVICE_ADVERTISE_PORT    — port override (default: 0 = use Register port)
//	WORKLOAD_ID               — workload scope for tag-based filtering (default: empty = no filter)
func ConfigFromEnv() Config {
	c := Config{
		Enabled:       os.Getenv("SERVICE_DISCOVERY_ENABLED") == "true",
		ConsulAddr:    envOrDefault("CONSUL_ADDR", "localhost:8500"),
		AdvertiseAddr: os.Getenv("SERVICE_ADVERTISE_ADDR"),
		AdvertisePort: envIntOrDefault("SERVICE_ADVERTISE_PORT", 0),
		Workload:      os.Getenv("WORKLOAD_ID"),
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

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}

	return fallback
}
