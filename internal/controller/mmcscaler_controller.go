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

package controller

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/redis/go-redis/v9"

	scalingv1alpha1 "github.com/will-m-0/queue-scaler/api/v1alpha1"
)

// MMcScalerReconciler reconciles a MMcScaler object
type MMcScalerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=scaling.will-m-0.github.io,resources=mmcscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scaling.will-m-0.github.io,resources=mmcscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scaling.will-m-0.github.io,resources=mmcscalers/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MMcScaler object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *MMcScalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var scaler scalingv1alpha1.MMcScaler
	if err := r.Get(ctx, req.NamespacedName, &scaler); err != nil {
		if apierrors.IsNotFound(err) {
			// custom resource not found - normally either deleted or not created
			log.Info("MMcScaler resource not found. Ignoring as either deleted or never created")
			return ctrl.Result{}, nil
		}
		// Error reading object - requeue request
		// request is requeued even though no field set on ctrl.Result{}, as err is not nil
		log.Error(err, "Failed to get MMcScaler")
		return ctrl.Result{}, err
	}

	// read fresh state
	var deploy appsv1.Deployment
	key := client.ObjectKey{
		Namespace: scaler.Namespace,
		Name:      scaler.Spec.TargetRef.Name,
	}
	if err := r.Get(ctx, key, &deploy); err != nil {
		if apierrors.IsNotFound(err) {
			// cannot find - wait for next reconciliation cycle
			log.Info("target deployment not found", "name", key.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// TOOD - check errors fromr redis and prom client creations

	// redis client for reading queue length
	// TODO - reuse same redis client across reconciliations
	rdb := redis.NewClient(&redis.Options{
		Addr: scaler.Spec.RedisAddress,
	})

	// prometheus client for reading job service length over window
	promClient, err := promapi.NewClient(promapi.Config{
		Address: scaler.Spec.PrometheusAddress,
	})
	prom := promv1.NewAPI(promClient)

	// read queue length from redis
	queue_length := rdb.LLen(ctx, scaler.Spec.QueueKey)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MMcScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&scalingv1alpha1.MMcScaler{}).
		Named("mmcscaler").
		Complete(r)
}
