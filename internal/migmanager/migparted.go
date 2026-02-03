package migmanager

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

type migPartedConfig struct {
	Version    string                   `json:"version" yaml:"version"`
	MigConfigs map[string][]interface{} `json:"mig-configs" yaml:"mig-configs"`
}

type migPartedConfigEntry struct {
	Devices    string           `json:"devices"`
	MigEnabled bool             `json:"mig-enabled"`
	MigDevices map[string]int64 `json:"mig-devices,omitempty"`
}

type migPartedConfigFile struct {
	Version    string                            `json:"version"`
	MigConfigs map[string][]migPartedConfigEntry `json:"mig-configs"`
}

func LoadMigConfigNames(path string) (map[string]struct{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	log.Printf("mig-parted: loaded config file path=%s bytes=%d", path, len(data))
	var cfg migPartedConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}
	names := make(map[string]struct{}, len(cfg.MigConfigs))
	for name := range cfg.MigConfigs {
		names[name] = struct{}{}
	}
	if len(names) == 0 {
		log.Printf("mig-parted: no mig-configs parsed from %s content=%s", path, truncateLogValue(string(data), 2048))
	} else {
		log.Printf("mig-parted: parsed mig-configs names=%s", strings.Join(sortedKeys(names), ","))
	}
	return names, nil
}

func BuildMigConfig(desired string, devices map[string]int64) ([]byte, error) {
	if desired == "" {
		return nil, fmt.Errorf("desired config name is required")
	}
	if len(devices) == 0 {
		return nil, fmt.Errorf("no mig devices requested")
	}
	entry := migPartedConfigEntry{
		Devices:    "all",
		MigEnabled: true,
		MigDevices: make(map[string]int64),
	}
	for profile, count := range devices {
		if count <= 0 {
			continue
		}
		entry.MigDevices[profile] = count
	}
	if len(entry.MigDevices) == 0 {
		return nil, fmt.Errorf("no positive mig device counts requested")
	}
	cfg := migPartedConfigFile{
		Version: "v1",
		MigConfigs: map[string][]migPartedConfigEntry{
			desired: {entry},
		},
	}
	return yaml.Marshal(cfg)
}

func ApplyMigConfig(ctx context.Context, cfg Config, desired string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.ApplyTimeout)
	defer cancel()

	args := []string{"apply", "-f", cfg.ConfigFile, "-c", desired}
	cmd := exec.CommandContext(ctx, cfg.MigPartedPath, args...)

	nvmlDir := os.Getenv("NVML_LIB_DIR")
	if nvmlDir == "" {
		nvmlDir = "/nvml"
	}

	env := os.Environ()
	env = append(env, fmt.Sprintf("HOST_ROOT_MOUNT=%s", cfg.HostRootMount))
	env = append(env, "LD_LIBRARY_PATH="+nvmlDir)
	cmd.Env = env

	log.Printf("mig-parted exec: path=%s args=%v", cfg.MigPartedPath, args)
	log.Printf("mig-parted env: HOST_ROOT_MOUNT=%s LD_LIBRARY_PATH=%s",
		cfg.HostRootMount, "/host/usr/lib/x86_64-linux-gnu",
	)

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func VerifyMigApply(ctx context.Context, cfg Config) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, cfg.NvidiaSmiPath, "-L")
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func truncateLogValue(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}
