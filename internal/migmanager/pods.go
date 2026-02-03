package migmanager

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/constants"
)

const podNodeNameField = "spec.nodeName"

func SetupFieldIndexers(mgr client.FieldIndexer) error {
	return mgr.IndexField(context.Background(), &corev1.Pod{}, podNodeNameField, func(obj client.Object) []string {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return nil
		}
		return []string{pod.Spec.NodeName}
	})
}

func listNodePods(ctx context.Context, kubeClient client.Client, nodeName string) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := kubeClient.List(ctx, podList, client.MatchingFields{podNodeNameField: nodeName}); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func filterGPUPods(pods []corev1.Pod) []corev1.Pod {
	result := make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		pod := pods[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if isMirrorPod(&pod) || isPodManagedByDaemonSet(&pod) {
			continue
		}
		if !podHasGPUResources(&pod) {
			continue
		}
		result = append(result, pod)
	}
	return result
}

func podHasGPUResources(pod *corev1.Pod) bool {
	return containerListHasGPUResources(pod.Spec.InitContainers) || containerListHasGPUResources(pod.Spec.Containers)
}

func containerListHasGPUResources(containers []corev1.Container) bool {
	for _, container := range containers {
		if resourceListHasGPU(container.Resources.Requests) || resourceListHasGPU(container.Resources.Limits) {
			return true
		}
	}
	return false
}

func resourceListHasGPU(resources corev1.ResourceList) bool {
	for name, quantity := range resources {
		if quantity.IsZero() {
			continue
		}
		resourceName := string(name)
		if resourceName == "nvidia.com/gpu" || strings.HasPrefix(resourceName, constants.NvidiaMigResourceNamePrefix) {
			return true
		}
	}
	return false
}

func isPodManagedByDaemonSet(pod *corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" && owner.APIVersion == "apps/v1" {
			return true
		}
	}
	return false
}

func isMirrorPod(pod *corev1.Pod) bool {
	_, ok := pod.Annotations[corev1.MirrorPodAnnotationKey]
	return ok
}

func evictPod(ctx context.Context, clientset kubernetes.Interface, pod corev1.Pod) error {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	}
	return clientset.PolicyV1().Evictions(pod.Namespace).Evict(ctx, eviction)
}
