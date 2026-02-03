package migmanager

import (
	"errors"
	"fmt"
	"os"
	"time"
)

type Config struct {
	NodeName                  string
	ConfigFile                string
	GPUClientsFile            string
	HostRootMount             string
	MigPartedPath             string
	NvidiaSmiPath             string
	LockFile                  string
	DefaultConfig             string
	MigConfigLabel            string
	MigStrategyLabel          string
	MigStateLabel             string
	MigForceLabel             string
	MigDrainLabel             string
	LastAppliedLabel          string
	LastAppliedTimeAnnotation string
	ErrorLabel                string
	ErrorMessageLabel         string
	DrainStartAnnotation      string
	WithReboot                bool
	VerifyApply               bool
	EnableGPUClients          bool
	PollInterval              time.Duration
	DrainPollInterval         time.Duration
	DrainTimeout              time.Duration
	ApplyTimeout              time.Duration
}

func (c Config) Validate() error {
	if c.NodeName == "" {
		return errors.New("node name is required")
	}
	if c.ConfigFile == "" {
		return errors.New("config-file is required")
	}
	if c.MigConfigLabel == "" || c.MigStateLabel == "" {
		return errors.New("mig label keys cannot be empty")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll-interval must be positive: %s", c.PollInterval)
	}
	if c.DrainPollInterval <= 0 {
		return fmt.Errorf("drain-poll-interval must be positive: %s", c.DrainPollInterval)
	}
	if c.DrainTimeout <= 0 {
		return fmt.Errorf("drain-timeout must be positive: %s", c.DrainTimeout)
	}
	if c.ApplyTimeout <= 0 {
		return fmt.Errorf("apply-timeout must be positive: %s", c.ApplyTimeout)
	}
	return nil
}

func EnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
