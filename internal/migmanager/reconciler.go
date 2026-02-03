package migmanager

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/sgl-project/ome/pkg/constants"
)

const migConditionType = "MIGReconfigure"

type NodeReconciler struct {
	client    client.Client
	clientset kubernetes.Interface
	recorder  record.EventRecorder
	cfg       Config
}

func NewNodeReconciler(client client.Client, clientset kubernetes.Interface, recorder record.EventRecorder, cfg Config) *NodeReconciler {
	return &NodeReconciler{
		client:    client,
		clientset: clientset,
		recorder:  recorder,
		cfg:       cfg,
	}
}

func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	pred := nodeNamePredicate(r.cfg.NodeName)
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithEventFilter(pred).
		Complete(r)
}

func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.Name != r.cfg.NodeName {
		return ctrl.Result{}, nil
	}
	logger := log.FromContext(ctx).WithValues("node", req.Name)
	node := &corev1.Node{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: r.cfg.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	result, err := r.reconcileNode(ctx, logger, node)
	if err != nil {
		logger.Error(err, "Reconcile failed")
	}
	return result, err
}

func (r *NodeReconciler) reconcileNode(ctx context.Context, logger logr.Logger, node *corev1.Node) (ctrl.Result, error) {
	desiredLabel := strings.TrimSpace(node.Labels[r.cfg.MigConfigLabel])
	desired := desiredLabel
	if desired == "" && r.cfg.DefaultConfig != "" {
		desired = r.cfg.DefaultConfig
	}
	if desired == "" {
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling MIG config",
		"desired", desired,
		"desiredLabel", desiredLabel,
		"defaultConfig", r.cfg.DefaultConfig,
		"configFile", r.cfg.ConfigFile,
		"configLabelKey", r.cfg.MigConfigLabel)

	strategy := strings.TrimSpace(node.Labels[r.cfg.MigStrategyLabel])
	if strategy == "none" && desired != constants.NvidiaMigConfigDisabledValue {
		return r.failWithState(ctx, node, "strategy-none", "mig strategy is none", "MIG strategy is none", true)
	}

	desiredDevices, invalidDevices := migDevicesFromLabels(node.Labels)
	if len(invalidDevices) > 0 {
		sort.Strings(invalidDevices)
		logger.Info("Ignoring invalid MIG device labels", "labels", invalidDevices)
	}
	if desired == constants.NvidiaMigConfigDisabledValue {
		desiredDevices = map[string]int64{}
	}

	configFile := r.cfg.ConfigFile
	if desired != constants.NvidiaMigConfigDisabledValue {
		useMixed := len(desiredDevices) > 0 || strings.EqualFold(strategy, "mixed")
		if useMixed {
			if len(desiredDevices) == 0 {
				logger.Info("MIG strategy mixed but no device labels found; using base config", "strategy", strategy)
			} else {
				configData, err := BuildMigConfig(desired, desiredDevices)
				if err != nil {
					return r.failWithState(ctx, node, "config-build-failed", err.Error(), "Failed to build mig config", true)
				}
				tempFile, err := os.CreateTemp("", "ome-mig-config-*.yaml")
				if err != nil {
					return r.failWithState(ctx, node, "config-write-failed", err.Error(), "Failed to write mig config", true)
				}
				if _, err := tempFile.Write(configData); err != nil {
					_ = tempFile.Close()
					return r.failWithState(ctx, node, "config-write-failed", err.Error(), "Failed to write mig config", true)
				}
				if err := tempFile.Close(); err != nil {
					return r.failWithState(ctx, node, "config-write-failed", err.Error(), "Failed to write mig config", true)
				}
				configFile = tempFile.Name()
				defer func() {
					_ = os.Remove(configFile)
				}()
				logger.Info("Generated MIG config from labels", "configFile", configFile, "devices", desiredDevices)
			}
		}
	}

	if configData, readErr := os.ReadFile(configFile); readErr != nil {
		logger.Error(readErr, "Failed to read MIG config file for logging", "configFile", configFile)
	} else {
		logger.Info("Loaded MIG config file content",
			"configFile", configFile,
			"bytes", len(configData),
			"content", truncateMessage(string(configData), 2048))
	}

	configNames, err := LoadMigConfigNames(configFile)
	if err != nil {
		return r.failWithState(ctx, node, "config-load-failed", err.Error(), "Failed to load mig config", true)
	}
	available := make([]string, 0, len(configNames))
	for name := range configNames {
		available = append(available, name)
	}
	sort.Strings(available)
	logger.Info("Loaded MIG config names", "count", len(available), "names", available)
	if _, ok := configNames[desired]; !ok {
		return r.failWithState(ctx, node, "config-not-found", fmt.Sprintf("config %s not found", desired), "MIG config not found", true)
	}

	desiredSignature := migConfigSignature(desired, desiredDevices)
	if node.Labels[r.cfg.LastAppliedLabel] == desiredSignature && node.Labels[r.cfg.MigStateLabel] == "success" {
		return ctrl.Result{}, nil
	}

	lock, err := AcquireFileLock(r.cfg.LockFile)
	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ctrl.Result{RequeueAfter: r.cfg.PollInterval}, nil
		}
		return ctrl.Result{}, err
	}
	defer func() {
		_ = lock.Release()
	}()

	forceApply := labelBool(node.Labels[r.cfg.MigForceLabel])
	drainAllowed := labelBool(node.Labels[r.cfg.MigDrainLabel])

	pods, err := listNodePods(ctx, r.client, r.cfg.NodeName)
	if err != nil {
		return r.failWithState(ctx, node, "pod-list-failed", err.Error(), "Failed to list pods", true)
	}
	gpuPods := filterGPUPods(pods)

	if len(gpuPods) > 0 && !forceApply {
		if drainAllowed {
			drained, drainErr := r.evictGpuPods(ctx, gpuPods, node)
			if drainErr != nil {
				return r.failWithState(ctx, node, "drain-failed", drainErr.Error(), "GPU pod drain failed", true)
			}
			if !drained {
				if err := r.setStatePending(ctx, node, "WaitingForDrain", "Waiting for GPU pods to terminate"); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: r.cfg.DrainPollInterval}, nil
			}
		} else {
			if err := r.setStatePending(ctx, node, "WaitingForPods", "GPU pods are still running"); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: r.cfg.PollInterval}, nil
		}
	}

	if err := r.setStateApplying(ctx, node, desired); err != nil {
		return ctrl.Result{}, err
	}
	r.recorder.Event(node, corev1.EventTypeNormal, "MIGReconfigureStarted", fmt.Sprintf("Applying MIG config %s", desired))

	applyCfg := r.cfg
	applyCfg.ConfigFile = configFile
	applyOutput, err := ApplyMigConfig(ctx, applyCfg, desired)
	if err != nil {
		logger.Error(err, "mig-parted apply failed", "output", applyOutput)
		return r.failWithState(ctx, node, "apply-failed", applyOutput, "MIG apply failed", true)
	}

	if r.cfg.VerifyApply {
		verifyOutput, verifyErr := VerifyMigApply(ctx, applyCfg)
		if verifyErr != nil {
			logger.Error(verifyErr, "nvidia-smi verification failed", "output", verifyOutput)
			return r.failWithState(ctx, node, "verify-failed", verifyOutput, "MIG verify failed", true)
		}
	}

	if err := r.setStateSuccess(ctx, node, desired, desiredSignature); err != nil {
		return ctrl.Result{}, err
	}
	r.recorder.Event(node, corev1.EventTypeNormal, "MIGReconfigureSucceeded", fmt.Sprintf("Applied MIG config %s", desired))
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) evictGpuPods(ctx context.Context, gpuPods []corev1.Pod, node *corev1.Node) (bool, error) {
	drainStart := node.Annotations[r.cfg.DrainStartAnnotation]
	if drainStart == "" {
		if err := r.setDrainStart(ctx, node, time.Now().UTC()); err != nil {
			return false, err
		}
	}

	if drainStart != "" {
		startTime, err := time.Parse(time.RFC3339, drainStart)
		if err == nil && time.Since(startTime) > r.cfg.DrainTimeout {
			return false, fmt.Errorf("drain timeout exceeded")
		}
	}

	for _, pod := range gpuPods {
		if err := evictPod(ctx, r.clientset, pod); err != nil {
			if apierrors.IsNotFound(err) || apierrors.IsTooManyRequests(err) || apierrors.IsForbidden(err) {
				continue
			}
			return false, err
		}
	}

	return false, nil
}

func (r *NodeReconciler) setStatePending(ctx context.Context, node *corev1.Node, reason, message string) error {
	return r.updateState(ctx, node, "pending", corev1.ConditionTrue, reason, message, "", "")
}

func (r *NodeReconciler) setStateApplying(ctx context.Context, node *corev1.Node, desired string) error {
	if err := r.updateState(ctx, node, "applying", corev1.ConditionTrue, "Applying", fmt.Sprintf("Applying %s", desired), "", ""); err != nil {
		return err
	}
	return nil
}

func (r *NodeReconciler) setStateSuccess(ctx context.Context, node *corev1.Node, desired, appliedSignature string) error {
	updates := map[string]string{
		r.cfg.LastAppliedLabel: appliedSignature,
	}
	annotations := map[string]string{
		r.cfg.LastAppliedTimeAnnotation: time.Now().UTC().Format(time.RFC3339),
	}
	if err := r.patchNodeLabels(ctx, node, updates, nil); err != nil {
		return err
	}
	deleteAnnotations := []string{}
	if r.cfg.DrainStartAnnotation != "" {
		deleteAnnotations = append(deleteAnnotations, r.cfg.DrainStartAnnotation)
	}
	if err := r.patchNodeAnnotations(ctx, node, annotations, deleteAnnotations); err != nil {
		return err
	}
	return r.updateState(ctx, node, "success", corev1.ConditionFalse, "Success", fmt.Sprintf("Applied %s", desired), "", "")
}

func (r *NodeReconciler) failWithState(ctx context.Context, node *corev1.Node, code, msg, reason string, requeue bool) (ctrl.Result, error) {
	prevState := node.Labels[r.cfg.MigStateLabel]
	prevError := node.Labels[r.cfg.ErrorLabel]
	nextError := sanitizeLabelValue(code)
	if err := r.updateState(ctx, node, "failed", corev1.ConditionUnknown, reason, msg, code, msg); err != nil {
		return ctrl.Result{}, err
	}
	if prevState != "failed" || prevError != nextError {
		r.recorder.Event(node, corev1.EventTypeWarning, "MIGReconfigureFailed", reason)
	}
	if requeue {
		return ctrl.Result{RequeueAfter: r.cfg.PollInterval}, nil
	}
	return ctrl.Result{}, nil
}

func (r *NodeReconciler) updateState(ctx context.Context, node *corev1.Node, state string, condStatus corev1.ConditionStatus, reason, message, errCode, errMessage string) error {
	labelUpdates := map[string]string{
		r.cfg.MigStateLabel: state,
	}
	labelDeletes := []string{}
	if errCode != "" {
		labelUpdates[r.cfg.ErrorLabel] = sanitizeLabelValue(errCode)
	} else if r.cfg.ErrorLabel != "" {
		labelDeletes = append(labelDeletes, r.cfg.ErrorLabel)
	}
	if errMessage != "" {
		labelUpdates[r.cfg.ErrorMessageLabel] = sanitizeLabelValue(errMessage)
	} else if r.cfg.ErrorMessageLabel != "" {
		labelDeletes = append(labelDeletes, r.cfg.ErrorMessageLabel)
	}

	if err := r.patchNodeLabels(ctx, node, labelUpdates, labelDeletes); err != nil {
		return err
	}

	condition := corev1.NodeCondition{
		Type:               migConditionType,
		Status:             condStatus,
		Reason:             reason,
		Message:            truncateMessage(message, 512),
		LastTransitionTime: metav1.Now(),
	}
	return r.patchNodeCondition(ctx, node, condition)
}

func (r *NodeReconciler) patchNodeLabels(ctx context.Context, node *corev1.Node, updates map[string]string, deletes []string) error {
	updated := node.DeepCopy()
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	for key, value := range updates {
		if key == "" {
			continue
		}
		updated.Labels[key] = value
	}
	for _, key := range deletes {
		delete(updated.Labels, key)
	}
	return r.client.Patch(ctx, updated, client.MergeFrom(node))
}

func (r *NodeReconciler) patchNodeAnnotations(ctx context.Context, node *corev1.Node, updates map[string]string, deletes []string) error {
	updated := node.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	for key, value := range updates {
		if key == "" {
			continue
		}
		updated.Annotations[key] = value
	}
	for _, key := range deletes {
		delete(updated.Annotations, key)
	}
	return r.client.Patch(ctx, updated, client.MergeFrom(node))
}

func (r *NodeReconciler) patchNodeCondition(ctx context.Context, node *corev1.Node, condition corev1.NodeCondition) error {
	updated := node.DeepCopy()
	conditions := updated.Status.Conditions
	found := false
	for i := range conditions {
		if conditions[i].Type == condition.Type {
			if conditions[i].Status != condition.Status {
				condition.LastTransitionTime = metav1.Now()
			} else {
				condition.LastTransitionTime = conditions[i].LastTransitionTime
			}
			conditions[i] = condition
			found = true
			break
		}
	}
	if !found {
		conditions = append(conditions, condition)
	}
	updated.Status.Conditions = conditions
	return r.client.Status().Patch(ctx, updated, client.MergeFrom(node))
}

func (r *NodeReconciler) setDrainStart(ctx context.Context, node *corev1.Node, start time.Time) error {
	if r.cfg.DrainStartAnnotation == "" {
		return nil
	}
	updates := map[string]string{
		r.cfg.DrainStartAnnotation: start.Format(time.RFC3339),
	}
	return r.patchNodeAnnotations(ctx, node, updates, nil)
}

func migDevicesFromLabels(labels map[string]string) (map[string]int64, []string) {
	devices := make(map[string]int64)
	invalid := make([]string, 0)
	for key, value := range labels {
		if !strings.HasPrefix(key, constants.NvidiaMigResourceNamePrefix) {
			continue
		}
		if value == "" {
			continue
		}
		count, err := strconv.ParseInt(value, 10, 64)
		if err != nil || count <= 0 {
			if value != "0" {
				invalid = append(invalid, fmt.Sprintf("%s=%s", key, value))
			}
			continue
		}
		profile := strings.TrimPrefix(key, constants.NvidiaMigResourceNamePrefix)
		devices[profile] = count
	}
	return devices, invalid
}

func migConfigSignature(desired string, devices map[string]int64) string {
	if len(devices) == 0 {
		return desired
	}
	pairs := make([]string, 0, len(devices))
	for profile, count := range devices {
		pairs = append(pairs, fmt.Sprintf("%s=%d", profile, count))
	}
	sort.Strings(pairs)
	sum := sha256.Sum256([]byte(desired + "|" + strings.Join(pairs, ",")))
	signature := fmt.Sprintf("%s-%x", desired, sum[:6])
	if len(signature) > 63 {
		return fmt.Sprintf("%x", sum[:12])
	}
	return signature
}

func labelBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes", "y":
		return true
	default:
		return false
	}
}

func sanitizeLabelValue(value string) string {
	if value == "" {
		return ""
	}
	buf := make([]rune, 0, len(value))
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			buf = append(buf, r)
		case r >= 'A' && r <= 'Z':
			buf = append(buf, r)
		case r >= '0' && r <= '9':
			buf = append(buf, r)
		case r == '-' || r == '_' || r == '.':
			buf = append(buf, r)
		default:
			buf = append(buf, '_')
		}
		if len(buf) >= 63 {
			break
		}
	}
	trimmed := strings.Trim(string(buf), "._-")
	if trimmed == "" {
		return "unknown"
	}
	if len(trimmed) > 63 {
		return trimmed[:63]
	}
	return trimmed
}

func truncateMessage(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}

func nodeNamePredicate(nodeName string) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetName() == nodeName
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetName() == nodeName
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return e.Object.GetName() == nodeName
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return e.Object.GetName() == nodeName
		},
	}
}
