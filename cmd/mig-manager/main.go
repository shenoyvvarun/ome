package main

import (
	"flag"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/sgl-project/ome/internal/migmanager"
	"github.com/sgl-project/ome/pkg/constants"
)

func main() {
	var (
		nodeName             string
		configFile           string
		gpuClientsFile       string
		hostRootMount        string
		migPartedPath        string
		nvidiaSmiPath        string
		lockFile             string
		defaultConfig        string
		migConfigLabel       string
		migStrategyLabel     string
		migStateLabel        string
		migForceLabel        string
		migDrainLabel        string
		lastAppliedLabel     string
		lastAppliedTimeAnno  string
		errorLabel           string
		errorMessageLabel    string
		drainStartAnnotation string
		withReboot           bool
		verifyApply          bool
		enableGPUClients     bool
		pollInterval         time.Duration
		drainPollInterval    time.Duration
		drainTimeout         time.Duration
		applyTimeout         time.Duration
		metricsAddr          string
		healthAddr           string
	)

	flag.StringVar(&nodeName, "node-name", migmanager.EnvOrDefault("NODE_NAME", ""), "Name of the node to reconcile.")
	flag.StringVar(&configFile, "config-file", "/mig-parted-config/config.yaml", "Path to mig-parted config file.")
	flag.StringVar(&gpuClientsFile, "gpu-clients-file", "", "Path to GPU clients config file (optional).")
	flag.StringVar(&hostRootMount, "host-root", migmanager.EnvOrDefault("HOST_ROOT_MOUNT", "/"), "Host root mount path.")
	flag.StringVar(&migPartedPath, "mig-parted-path", "nvidia-mig-parted", "Path to nvidia-mig-parted binary.")
	flag.StringVar(&nvidiaSmiPath, "nvidia-smi-path", "nvidia-smi", "Path to nvidia-smi binary.")
	flag.StringVar(&lockFile, "lock-file", "/tmp/ome-mig-manager.lock", "Lock file path to serialize MIG changes.")
	flag.StringVar(&defaultConfig, "default-config", "", "Default MIG config to apply when label is missing.")

	flag.StringVar(&migConfigLabel, "mig-config-label", migmanager.EnvOrDefault("MIG_CONFIG_LABEL", constants.NvidiaMigConfigLabel), "Node label for desired MIG config.")
	flag.StringVar(&migStrategyLabel, "mig-strategy-label", migmanager.EnvOrDefault("MIG_STRATEGY_LABEL", "nvidia.com/mig.strategy"), "Node label for MIG strategy.")
	flag.StringVar(&migStateLabel, "mig-state-label", migmanager.EnvOrDefault("MIG_STATE_LABEL", constants.NvidiaMigConfigStateLabel), "Node label for MIG config state.")
	flag.StringVar(&migForceLabel, "mig-force-label", migmanager.EnvOrDefault("MIG_FORCE_LABEL", "your.domain/mig.force"), "Node label to force MIG reconfig.")
	flag.StringVar(&migDrainLabel, "mig-drain-label", migmanager.EnvOrDefault("MIG_DRAIN_LABEL", "your.domain/mig.drain"), "Node label to allow MIG drain.")
	flag.StringVar(&lastAppliedLabel, "mig-last-applied-label", migmanager.EnvOrDefault("MIG_LAST_APPLIED_LABEL", "your.domain/mig.lastApplied"), "Node label for last applied config.")
	flag.StringVar(&lastAppliedTimeAnno, "mig-last-applied-time-annotation", migmanager.EnvOrDefault("MIG_LAST_APPLIED_TIME_ANNOTATION", "your.domain/mig.lastAppliedTime"), "Node annotation for last applied time.")
	flag.StringVar(&errorLabel, "mig-error-label", migmanager.EnvOrDefault("MIG_ERROR_LABEL", "your.domain/mig.error"), "Node label for short error code.")
	flag.StringVar(&errorMessageLabel, "mig-error-message-label", migmanager.EnvOrDefault("MIG_ERROR_MESSAGE_LABEL", "your.domain/mig.errorMessage"), "Node label for short error message.")
	flag.StringVar(&drainStartAnnotation, "mig-drain-start-annotation", migmanager.EnvOrDefault("MIG_DRAIN_START_ANNOTATION", "your.domain/mig.drainStart"), "Node annotation to store drain start time.")

	flag.BoolVar(&withReboot, "with-reboot", false, "Allow mig-parted to request reboot if needed.")
	flag.BoolVar(&verifyApply, "verify", true, "Run nvidia-smi -L after applying config.")
	flag.BoolVar(&enableGPUClients, "enable-gpu-clients", false, "Attempt to stop GPU client services when systemd is available.")

	flag.DurationVar(&pollInterval, "poll-interval", 30*time.Second, "Requeue interval when waiting.")
	flag.DurationVar(&drainPollInterval, "drain-poll-interval", 10*time.Second, "Drain polling interval.")
	flag.DurationVar(&drainTimeout, "drain-timeout", 10*time.Minute, "Maximum time to wait for GPU pods to drain.")
	flag.DurationVar(&applyTimeout, "apply-timeout", 10*time.Minute, "Timeout for mig-parted apply.")

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "Metrics bind address; set to 0 to disable.")
	flag.StringVar(&healthAddr, "health-probe-bind-address", ":8081", "Health probe bind address.")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	if nodeName == "" {
		ctrl.Log.Error(nil, "node-name is required")
		os.Exit(1)
	}

	cfg := migmanager.Config{
		NodeName:                  nodeName,
		ConfigFile:                configFile,
		GPUClientsFile:            gpuClientsFile,
		HostRootMount:             hostRootMount,
		MigPartedPath:             migPartedPath,
		NvidiaSmiPath:             nvidiaSmiPath,
		LockFile:                  lockFile,
		DefaultConfig:             defaultConfig,
		MigConfigLabel:            migConfigLabel,
		MigStrategyLabel:          migStrategyLabel,
		MigStateLabel:             migStateLabel,
		MigForceLabel:             migForceLabel,
		MigDrainLabel:             migDrainLabel,
		LastAppliedLabel:          lastAppliedLabel,
		LastAppliedTimeAnnotation: lastAppliedTimeAnno,
		ErrorLabel:                errorLabel,
		ErrorMessageLabel:         errorMessageLabel,
		DrainStartAnnotation:      drainStartAnnotation,
		WithReboot:                withReboot,
		VerifyApply:               verifyApply,
		EnableGPUClients:          enableGPUClients,
		PollInterval:              pollInterval,
		DrainPollInterval:         drainPollInterval,
		DrainTimeout:              drainTimeout,
		ApplyTimeout:              applyTimeout,
	}
	if err := cfg.Validate(); err != nil {
		ctrl.Log.Error(err, "Invalid configuration")
		os.Exit(1)
	}

	scheme := clientgoscheme.Scheme
	if err := corev1.AddToScheme(scheme); err != nil {
		ctrl.Log.Error(err, "Failed to add corev1 scheme")
		os.Exit(1)
	}

	restCfg := ctrl.GetConfigOrDie()
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		ctrl.Log.Error(err, "Failed to create kubernetes clientset")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: healthAddr,
		LeaderElection:         false,
	})
	if err != nil {
		ctrl.Log.Error(err, "Failed to create manager")
		os.Exit(1)
	}

	if err := migmanager.SetupFieldIndexers(mgr.GetFieldIndexer()); err != nil {
		ctrl.Log.Error(err, "Failed to set up field indexers")
		os.Exit(1)
	}

	reconciler := migmanager.NewNodeReconciler(mgr.GetClient(), clientset, mgr.GetEventRecorderFor("ome-mig-manager"), cfg)
	if err := reconciler.SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "Failed to set up reconciler")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "Failed to set up health check")
		os.Exit(1)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "Manager exited with error")
		os.Exit(1)
	}
}
