package inferenceservice

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1beta1 "github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
)

type migAssignment struct {
	Node     string `json:"node"`
	Config   string `json:"config"`
	Resource string `json:"resource"`
	Applied  bool   `json:"applied"`
}

type migRequest struct {
	Deployment   string
	Resource     string
	Quantity     int64
	NodeSelector map[string]string
	Affinity     *corev1.Affinity
	Tolerations  []corev1.Toleration
}

type migNodeKey struct {
	Node   string
	Config string
}

type migNodeResourceKey struct {
	Node     string
	Resource string
}

func (r *InferenceServiceReconciler) reconcileMIGProfiles(ctx context.Context, isvc *v1beta1.InferenceService) error {
	log := log.FromContext(ctx)
	requests, err := r.collectMIGRequests(ctx, isvc)
	if err != nil {
		return err
	}
	log.Info("Collected MIG requests", "inferenceService", isvc.Name, "namespace", isvc.Namespace, "count", len(requests))

	assignments, err := getMigAssignments(isvc)
	if err != nil {
		return err
	}
	log.Info("Loaded MIG assignments", "inferenceService", isvc.Name, "namespace", isvc.Namespace, "count", len(assignments))

	desired := make(map[string]migRequest, len(requests))
	for _, req := range requests {
		desired[req.Deployment] = req
	}

	updated := false
	removed := make(map[string]migAssignment)

	for name, existing := range assignments {
		if _, ok := desired[name]; !ok {
			log.Info("Removing stale MIG assignment", "deployment", name, "node", existing.Node, "resource", existing.Resource, "config", existing.Config)
			removed[name] = existing
			delete(assignments, name)
			updated = true
		}
	}

	for _, req := range requests {
		config := migConfigForResource(req.Resource)
		existing, ok := assignments[req.Deployment]
		if ok && existing.Resource == req.Resource && existing.Node != "" {
			if existing.Config == "" || existing.Config != config {
				log.Info("Updating MIG assignment config", "deployment", req.Deployment, "node", existing.Node, "resource", req.Resource, "oldConfig", existing.Config, "newConfig", config)
				existing.Config = config
				updated = true
			}
			if existing.Applied {
				if _, err := r.ensureNodeMigConfig(ctx, existing.Node, existing.Config); err != nil {
					return err
				}
			}
			if _, err := r.ensureNodeMigResourceLabel(ctx, existing.Node, req.Resource, req.Quantity); err != nil {
				return err
			}
			assignments[req.Deployment] = existing
			continue
		}

		nodeName, applyLabel, err := r.selectNodeForMIG(ctx, req, config)
		if err != nil {
			return err
		}
		log.Info("Selected MIG node for request", "deployment", req.Deployment, "resource", req.Resource, "quantity", req.Quantity, "node", nodeName, "config", config, "applyLabel", applyLabel)

		applied := false
		if applyLabel {
			applied, err = r.ensureNodeMigConfig(ctx, nodeName, config)
			if err != nil {
				return err
			}
			if applied {
				log.Info("Applied MIG config label", "node", nodeName, "config", config)
			}
		}
		if _, err := r.ensureNodeMigResourceLabel(ctx, nodeName, req.Resource, req.Quantity); err != nil {
			return err
		}

		assignments[req.Deployment] = migAssignment{
			Node:     nodeName,
			Config:   config,
			Resource: req.Resource,
			Applied:  applied,
		}
		updated = true
	}

	if len(removed) > 0 {
		log.Info("Releasing MIG assignments", "inferenceService", isvc.Name, "namespace", isvc.Namespace, "count", len(removed))
		if err := r.releaseMigAssignments(ctx, isvc, removed); err != nil {
			return err
		}
	}

	if !updated {
		log.Info("MIG assignments unchanged", "inferenceService", isvc.Name, "namespace", isvc.Namespace)
		return nil
	}

	log.Info("Updating MIG assignments", "inferenceService", isvc.Name, "namespace", isvc.Namespace, "count", len(assignments))
	return r.updateMigAssignmentsAnnotation(ctx, isvc, assignments)
}

func (r *InferenceServiceReconciler) cleanupMIGProfiles(ctx context.Context, isvc *v1beta1.InferenceService) error {
	log := log.FromContext(ctx)
	assignments, err := getMigAssignments(isvc)
	if err != nil || len(assignments) == 0 {
		return err
	}

	log.Info("Cleaning up MIG assignments", "inferenceService", isvc.Name, "namespace", isvc.Namespace, "count", len(assignments))
	if err := r.releaseMigAssignments(ctx, isvc, assignments); err != nil {
		return err
	}

	return nil
}

func (r *InferenceServiceReconciler) releaseMigAssignments(ctx context.Context, isvc *v1beta1.InferenceService, assignments map[string]migAssignment) error {
	log := log.FromContext(ctx)
	otherAssignments, err := r.collectOtherMigAssignments(ctx, isvc)
	if err != nil {
		return err
	}
	otherResourceAssignments, err := r.collectOtherMigResourceAssignments(ctx, isvc)
	if err != nil {
		return err
	}

	for _, assignment := range assignments {
		if !assignment.Applied || assignment.Node == "" || assignment.Config == "" {
			continue
		}
		key := migNodeKey{Node: assignment.Node, Config: assignment.Config}
		if otherAssignments[key] {
			log.Info("Skipping MIG reset; config still in use", "node", assignment.Node, "config", assignment.Config)
			continue
		}
		log.Info("Resetting MIG config", "node", assignment.Node, "config", assignment.Config)
		if err := r.resetNodeMigConfig(ctx, assignment.Node, assignment.Config); err != nil {
			return err
		}
	}

	for _, assignment := range assignments {
		if assignment.Node == "" || assignment.Resource == "" {
			continue
		}
		key := migNodeResourceKey{Node: assignment.Node, Resource: assignment.Resource}
		if otherResourceAssignments[key] {
			log.Info("Skipping MIG resource label reset; resource still in use", "node", assignment.Node, "resource", assignment.Resource)
			continue
		}
		log.Info("Resetting MIG resource label", "node", assignment.Node, "resource", assignment.Resource)
		if err := r.resetNodeMigResourceLabel(ctx, assignment.Node, assignment.Resource); err != nil {
			return err
		}
	}

	return nil
}

func (r *InferenceServiceReconciler) collectOtherMigAssignments(ctx context.Context, isvc *v1beta1.InferenceService) (map[migNodeKey]bool, error) {
	result := make(map[migNodeKey]bool)

	isvcList := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, isvcList); err != nil {
		return nil, err
	}

	for i := range isvcList.Items {
		item := &isvcList.Items[i]
		if item.UID == isvc.UID {
			continue
		}
		if !item.DeletionTimestamp.IsZero() {
			continue
		}
		assignments, err := getMigAssignments(item)
		if err != nil {
			continue
		}
		for _, assignment := range assignments {
			if assignment.Node == "" || assignment.Config == "" {
				continue
			}
			result[migNodeKey{Node: assignment.Node, Config: assignment.Config}] = true
		}
	}

	return result, nil
}

func (r *InferenceServiceReconciler) collectOtherMigResourceAssignments(ctx context.Context, isvc *v1beta1.InferenceService) (map[migNodeResourceKey]bool, error) {
	result := make(map[migNodeResourceKey]bool)

	isvcList := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, isvcList); err != nil {
		return nil, err
	}

	for i := range isvcList.Items {
		item := &isvcList.Items[i]
		if item.UID == isvc.UID {
			continue
		}
		if !item.DeletionTimestamp.IsZero() {
			continue
		}
		assignments, err := getMigAssignments(item)
		if err != nil {
			continue
		}
		for _, assignment := range assignments {
			if assignment.Node == "" || assignment.Resource == "" {
				continue
			}
			result[migNodeResourceKey{Node: assignment.Node, Resource: assignment.Resource}] = true
		}
	}

	return result, nil
}

func (r *InferenceServiceReconciler) collectMIGRequests(ctx context.Context, isvc *v1beta1.InferenceService) ([]migRequest, error) {
	deployments := &appsv1.DeploymentList{}
	if err := r.List(ctx, deployments,
		client.InNamespace(isvc.Namespace),
		client.MatchingLabels{constants.InferenceServicePodLabelKey: isvc.Name},
	); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	var requests []migRequest
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		if !deploymentOwnedByISVC(deployment, isvc) {
			continue
		}
		resourceRequests := getMigResourceRequests(&deployment.Spec.Template.Spec)
		for resource, qty := range resourceRequests {
			requests = append(requests, migRequest{
				Deployment:   deployment.Name,
				Resource:     resource,
				Quantity:     qty,
				NodeSelector: deployment.Spec.Template.Spec.NodeSelector,
				Affinity:     deployment.Spec.Template.Spec.Affinity,
				Tolerations:  deployment.Spec.Template.Spec.Tolerations,
			})
		}
	}

	return requests, nil
}

func deploymentOwnedByISVC(deployment *appsv1.Deployment, isvc *v1beta1.InferenceService) bool {
	for _, ref := range deployment.GetOwnerReferences() {
		if ref.Kind == "InferenceService" &&
			ref.APIVersion == v1beta1.SchemeGroupVersion.String() &&
			ref.Name == isvc.Name &&
			ref.UID == isvc.UID {
			return true
		}
	}
	return false
}

func getMigResourceRequests(podSpec *corev1.PodSpec) map[string]int64 {
	requests := make(map[string]int64)
	if podSpec == nil {
		return requests
	}

	for _, container := range podSpec.Containers {
		containerRequests := getMigResourceRequestsForContainer(container)
		for resource, qty := range containerRequests {
			requests[resource] += qty
		}
	}

	return requests
}

func getMigResourceRequestsForContainer(container corev1.Container) map[string]int64 {
	requests := make(map[string]int64)
	resourceNames := make(map[corev1.ResourceName]struct{})

	for name := range container.Resources.Requests {
		if isMigResourceName(name) {
			resourceNames[name] = struct{}{}
		}
	}
	for name := range container.Resources.Limits {
		if isMigResourceName(name) {
			resourceNames[name] = struct{}{}
		}
	}

	for name := range resourceNames {
		req := container.Resources.Requests[name]
		limit := container.Resources.Limits[name]
		reqQty := req.Value()
		limitQty := limit.Value()
		if limitQty > reqQty {
			reqQty = limitQty
		}
		if reqQty > 0 {
			requests[string(name)] = reqQty
		}
	}

	return requests
}

func (r *InferenceServiceReconciler) selectNodeForMIG(ctx context.Context, req migRequest, config string) (string, bool, error) {
	log := log.FromContext(ctx)
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return "", false, err
	}
	log.Info("Evaluating MIG candidates", "deployment", req.Deployment, "resource", req.Resource, "quantity", req.Quantity, "nodes", len(nodeList.Items))

	var candidates []*corev1.Node
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if !nodeMatchesSelector(node, req.NodeSelector) {
			continue
		}
		if !nodeMatchesRequiredAffinity(node, req.Affinity) {
			continue
		}
		if !nodeToleratesTaints(node, req.Tolerations) {
			continue
		}
		if !nodeIsMigCapable(node) {
			continue
		}
		candidates = append(candidates, node)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Name < candidates[j].Name
	})

	for _, node := range candidates {
		if nodeMigConfigStateReady(node) && node.Labels[constants.NvidiaMigConfigLabel] == config {
			if nodeHasMigCapacity(node, req.Resource, req.Quantity) {
				log.Info("Selected node with existing MIG config", "deployment", req.Deployment, "node", node.Name, "config", config, "resource", req.Resource)
				return node.Name, false, nil
			}
		}
	}

	for _, node := range candidates {
		value := node.Labels[constants.NvidiaMigConfigLabel]
		if value == "" || value == constants.NvidiaMigConfigDisabledValue {
			log.Info("Selected node for MIG config apply", "deployment", req.Deployment, "node", node.Name, "config", config, "resource", req.Resource)
			return node.Name, true, nil
		}
	}

	log.Info("No eligible node found for MIG request",
		"inferenceService", req.Deployment,
		"resource", req.Resource,
		"quantity", req.Quantity,
		"config", config)

	return "", false, fmt.Errorf("no nodes available for mig resource %s", req.Resource)
}

func (r *InferenceServiceReconciler) ensureNodeMigConfig(ctx context.Context, nodeName, config string) (bool, error) {
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return false, err
	}

	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}

	current := node.Labels[constants.NvidiaMigConfigLabel]
	if current == config {
		return false, nil
	}

	original := node.DeepCopy()
	node.Labels[constants.NvidiaMigConfigLabel] = config
	if err := r.Patch(ctx, node, client.MergeFrom(original)); err != nil {
		return false, err
	}

	return true, nil
}

func (r *InferenceServiceReconciler) ensureNodeMigResourceLabel(ctx context.Context, nodeName, resource string, quantity int64) (bool, error) {
	if quantity < 0 {
		quantity = 0
	}
	if !isMigResourceName(corev1.ResourceName(resource)) {
		return false, nil
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return false, err
	}
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	desired := ""
	if quantity > 0 {
		desired = fmt.Sprintf("%d", quantity)
	}
	current := node.Labels[resource]
	if current == desired {
		return false, nil
	}
	original := node.DeepCopy()
	if desired == "" {
		delete(node.Labels, resource)
	} else {
		node.Labels[resource] = desired
	}
	if err := r.Patch(ctx, node, client.MergeFrom(original)); err != nil {
		return false, err
	}
	return true, nil
}

func (r *InferenceServiceReconciler) resetNodeMigConfig(ctx context.Context, nodeName, config string) error {
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return client.IgnoreNotFound(err)
	}
	current := node.Labels[constants.NvidiaMigConfigLabel]
	if current != config {
		return nil
	}

	original := node.DeepCopy()
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	node.Labels[constants.NvidiaMigConfigLabel] = constants.NvidiaMigConfigDisabledValue
	return r.Patch(ctx, node, client.MergeFrom(original))
}

func (r *InferenceServiceReconciler) resetNodeMigResourceLabel(ctx context.Context, nodeName, resource string) error {
	if !isMigResourceName(corev1.ResourceName(resource)) {
		return nil
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return client.IgnoreNotFound(err)
	}
	if node.Labels == nil {
		return nil
	}
	if _, ok := node.Labels[resource]; !ok {
		return nil
	}
	original := node.DeepCopy()
	delete(node.Labels, resource)
	return r.Patch(ctx, node, client.MergeFrom(original))
}

func updateMigAssignmentsAnnotation(isvc *v1beta1.InferenceService, assignments map[string]migAssignment) error {
	if len(assignments) == 0 {
		delete(isvc.Annotations, constants.MigAssignmentsAnnotationKey)
		return nil
	}
	data, err := json.Marshal(assignments)
	if err != nil {
		return err
	}
	if isvc.Annotations == nil {
		isvc.Annotations = make(map[string]string)
	}
	isvc.Annotations[constants.MigAssignmentsAnnotationKey] = string(data)
	return nil
}

func (r *InferenceServiceReconciler) updateMigAssignmentsAnnotation(ctx context.Context, isvc *v1beta1.InferenceService, assignments map[string]migAssignment) error {
	original := isvc.DeepCopy()
	if err := updateMigAssignmentsAnnotation(isvc, assignments); err != nil {
		return err
	}
	return r.Patch(ctx, isvc, client.MergeFrom(original))
}

func getMigAssignments(isvc *v1beta1.InferenceService) (map[string]migAssignment, error) {
	raw := ""
	if isvc.Annotations != nil {
		raw = isvc.Annotations[constants.MigAssignmentsAnnotationKey]
	}
	if raw == "" {
		return map[string]migAssignment{}, nil
	}
	assignments := make(map[string]migAssignment)
	if err := json.Unmarshal([]byte(raw), &assignments); err != nil {
		return nil, err
	}
	return assignments, nil
}

func migConfigForResource(resource string) string {
	if strings.HasPrefix(resource, constants.NvidiaMigResourceNamePrefix) {
		return "all-" + strings.TrimPrefix(resource, constants.NvidiaMigResourceNamePrefix)
	}
	return resource
}

func isMigResourceName(name corev1.ResourceName) bool {
	return strings.HasPrefix(string(name), constants.NvidiaMigResourceNamePrefix)
}

func nodeIsMigCapable(node *corev1.Node) bool {
	value, ok := node.Labels[constants.NvidiaMigCapableLabel]
	if !ok || value == "" {
		return true
	}
	value = strings.ToLower(value)
	return value == "true" || value == "1" || value == "yes"
}

func nodeMigConfigStateReady(node *corev1.Node) bool {
	value, ok := node.Labels[constants.NvidiaMigConfigStateLabel]
	if !ok || value == "" {
		return true
	}
	return strings.EqualFold(value, "success")
}

func nodeHasMigCapacity(node *corev1.Node, resource string, quantity int64) bool {
	if quantity <= 0 {
		return true
	}
	allocatable := node.Status.Allocatable[corev1.ResourceName(resource)]
	return allocatable.Value() >= quantity
}

func nodeMatchesSelector(node *corev1.Node, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	for k, v := range selector {
		if node.Labels[k] != v {
			return false
		}
	}
	return true
}

func nodeMatchesRequiredAffinity(node *corev1.Node, affinity *corev1.Affinity) bool {
	if affinity == nil || affinity.NodeAffinity == nil || affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	selector := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	for _, term := range selector.NodeSelectorTerms {
		if matchNodeSelectorTerm(node, term) {
			return true
		}
	}
	return false
}

func matchNodeSelectorTerm(node *corev1.Node, term corev1.NodeSelectorTerm) bool {
	if len(term.MatchExpressions) > 0 {
		for _, req := range term.MatchExpressions {
			if !matchNodeSelectorExpressions(node, req) {
				return false
			}
		}
	}
	if len(term.MatchFields) > 0 {
		for _, req := range term.MatchFields {
			if !matchNodeSelectorFields(node, req) {
				return false
			}
		}
	}
	return true
}

func matchNodeSelectorExpressions(node *corev1.Node, req corev1.NodeSelectorRequirement) bool {
	val, has := node.Labels[req.Key]
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		if !has {
			return false
		}
		for _, v := range req.Values {
			if v == val {
				return true
			}
		}
		return false
	case corev1.NodeSelectorOpNotIn:
		if !has {
			return true
		}
		for _, v := range req.Values {
			if v == val {
				return false
			}
		}
		return true
	case corev1.NodeSelectorOpExists:
		return has
	case corev1.NodeSelectorOpDoesNotExist:
		return !has
	case corev1.NodeSelectorOpGt:
		if !has || len(req.Values) == 0 {
			return false
		}
		return strings.Compare(val, req.Values[0]) > 0
	case corev1.NodeSelectorOpLt:
		if !has || len(req.Values) == 0 {
			return false
		}
		return strings.Compare(val, req.Values[0]) < 0
	default:
		return true
	}
}

func matchNodeSelectorFields(node *corev1.Node, req corev1.NodeSelectorRequirement) bool {
	val, has := extractNodeFields(node)[req.Key]
	if !has {
		return false
	}
	for _, v := range req.Values {
		if v == val {
			return true
		}
	}
	return false
}

func extractNodeFields(n *corev1.Node) fields.Set {
	f := make(fields.Set)
	if len(n.Name) > 0 {
		f["metadata.name"] = n.Name
	}
	return f
}

func nodeToleratesTaints(node *corev1.Node, tolerations []corev1.Toleration) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectPreferNoSchedule {
			continue
		}
		if !toleratesTaint(tolerations, taint) {
			return false
		}
	}
	return true
}

func toleratesTaint(tolerations []corev1.Toleration, taint corev1.Taint) bool {
	for _, tol := range tolerations {
		if tolerationMatchesTaint(tol, taint) {
			return true
		}
	}
	return false
}

func tolerationMatchesTaint(tol corev1.Toleration, taint corev1.Taint) bool {
	if tol.Effect != "" && tol.Effect != taint.Effect {
		return false
	}

	switch tol.Operator {
	case corev1.TolerationOpExists:
		if tol.Key == "" {
			return true
		}
		return tol.Key == taint.Key
	case corev1.TolerationOpEqual, "":
		if tol.Key != taint.Key {
			return false
		}
		return tol.Value == taint.Value
	default:
		return false
	}
}
