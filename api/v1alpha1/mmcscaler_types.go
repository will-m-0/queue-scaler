/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// MMcScalerSpec defines the desired state of MMcScaler
type MMcScalerSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// Name of deployment to scale, must be in same namespace
	// +kubebuilder:validation:Required
	TargetRef corev1.LocalObjectReference `json:"targetRef"`

	// +kubebuilder:validation:Minimum=1
	MinReplicas int32 `json:"minReplicas"`
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas"`

	// this could drift from the env var setting workers in deployment - add validation inside controller code
	// +kubebuilder:validation:Required
	WorkersPerPod int32 `json:"workersPerPod"`

	// target utilisation of workers per pod. milli-units: 800 = 0.80
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	// +kubebuilder:default=700
	TargetUtilizationMilli int32 `json:"targetUtilizationMilli"`

	// T_d: desired backlog drain time once new capacity lands.
	// +kubebuilder:validation:Required
	DrainTime metav1.Duration `json:"drainTime"`

	// +kubebuilder:validation:Required
	RedisAddress string `json:"redisAddress"`

	// Redis list key
	// +kubebuilder:validation:Required
	QueueKey string `json:"queueKey"`

	// +kubebuilder:validation:Required
	PrometheusAddress string `json:"prometheusAddress"`

	// compute and record decisions without writing replicas
	// +kubebuilder:default=false
	DryRun bool `json:"dryRun,omitempty"`
}

// MMcScalerStatus defines the observed state of MMcScaler.
type MMcScalerStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	CurrentReplicas int32 `json:"currentReplicas"`
	DesiredReplicas int32 `json:"desiredReplicas"`

	ObservedQueueDepth       int32 `json:"observedQueueDepth"`
	ObservedUtilizationMilli int32 `json:"observedUtilizationMilli"`

	LastDecisionTime *metav1.Time `json:"lastDecisionTime"`
	LastScaleTime    *metav1.Time `json:"lastScaleTime"`

	// conditions represent the current state of the MMcScaler resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// MMcScaler is the Schema for the mmcscalers API
type MMcScaler struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of MMcScaler
	// +required
	Spec MMcScalerSpec `json:"spec"`

	// status defines the observed state of MMcScaler
	// +optional
	Status MMcScalerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// MMcScalerList contains a list of MMcScaler
type MMcScalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []MMcScaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &MMcScaler{}, &MMcScalerList{})
		return nil
	})
}
