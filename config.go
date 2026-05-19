package libsd

import (
	"os"
	"strconv"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
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
	// Required when Enabled is true. Typically the pod/container hostname or
	// the Ingress DNS name in Kubernetes.
	AdvertiseAddr string

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
//	SERVICE_ADVERTISE_ADDR    — address this instance advertises
//	SERVICE_ADVERTISE_PORT    — port override (default: 0 = use Register port)
//	WORKLOAD_ID               — workload scope for tag-based filtering (default: empty = no filter)
func ConfigFromEnv() Config {
	return Config{
		Enabled:       os.Getenv("SERVICE_DISCOVERY_ENABLED") == "true",
		ConsulAddr:    envOrDefault("CONSUL_ADDR", "localhost:8500"),
		AdvertiseAddr: os.Getenv("SERVICE_ADVERTISE_ADDR"),
		AdvertisePort: envIntOrDefault("SERVICE_ADVERTISE_PORT", 0),
		Workload:      os.Getenv("WORKLOAD_ID"),
	}
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
