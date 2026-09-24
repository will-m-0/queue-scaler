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
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/redis/go-redis/v9"

	scalingv1alpha1 "github.com/will-m-0/queue-scaler/api/v1alpha1"
	"github.com/will-m-0/queue-scaler/internal/promq"
)

// MMcScalerReconciler reconciles a MMcScaler object
type MMcScalerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	redis  *redis.Client
	prom   *promq.Client
}

// +kubebuilder:rbac:groups=scaling.will-m-0.github.io,resources=mmcscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scaling.will-m-0.github.io,resources=mmcscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scaling.will-m-0.github.io,resources=mmcscalers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch

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
	reconciliationStart := time.Now()
	log := logf.FromContext(ctx)

	var scaler scalingv1alpha1.MMcScaler
	if err := r.Get(ctx, req.NamespacedName, &scaler); err != nil {
		if apierrors.IsNotFound(err) {
			// custom resource not found - normally either deleted or not created
			log.Info("MMcScaler resource not found. Ignoring as either deleted or never created")

			// not an error, but dont retry
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
			// cannot find deployment - wait for next reconciliation cycle
			log.Info("target deployment not found", "name", key.Name)
			timeToNextTick := time.Duration(scaler.Spec.ReconciliationPeriod)*time.Second - time.Since(reconciliationStart)
			return ctrl.Result{
				RequeueAfter: timeToNextTick,
			}, nil
		}
		return ctrl.Result{}, err
	}

	if r.redis == nil || scaler.Spec.RedisAddress != r.redis.Options().Addr {
		if r.redis != nil {
			log.Info("redis addr changed, client is stale",
				"have", r.redis.Options().Addr, "want", scaler.Spec.RedisAddress)
			if err := r.redis.Close(); err != nil {
				log.Error(err, "closing stale redis client")
			}
		}
		r.redis = redis.NewClient(&redis.Options{Addr: scaler.Spec.RedisAddress})
	}

	const promQueryTimeout = 6 * time.Second
	if r.prom == nil || r.prom.Addr() != scaler.Spec.PrometheusAddress {
		if r.prom != nil {
			log.Info("prometheus address changed, client is stale",
				"have", r.prom.Addr(), "want", scaler.Spec.PrometheusAddress)
		}
		promClient, err := promq.New(scaler.Spec.PrometheusAddress, promQueryTimeout)
	if err != nil {
			// if failed to make prom client, config issue so on point retrying
		log.Error(err, "Failed to create prometheus client")
			return ctrl.Result{}, reconcile.TerminalError(fmt.Errorf("prometheus client: %w", err))
		}
		r.prom = promClient
	}

	observer := r.observerFor(scaler)
	observation, status, err := observer.observe(ctx, deploy, reconciliationStart)
	switch status {
	case obsIncomplete:
		// update status, and requeue without making scaling decision
		timeToNextTick := time.Duration(scaler.Spec.ReconciliationPeriod)*time.Second - time.Since(reconciliationStart)
		return ctrl.Result{
			RequeueAfter: timeToNextTick,
		}, nil
	case obsMisconfigured:
		// spec or image misconfigured, so reconcile will not succeed until image or spec change
		return ctrl.Result{}, reconcile.TerminalError(err)
	}

	// TODO - log observation
	log.Info("successful observation", observation)

	timeToNextTick := time.Duration(scaler.Spec.ReconciliationPeriod)*time.Second - time.Since(reconciliationStart)
	return ctrl.Result{
		RequeueAfter: timeToNextTick,
	}, nil
}

func (r *MMcScalerReconciler) Close() error {
	if r.redis != nil {
		return r.redis.Close()
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MMcScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&scalingv1alpha1.MMcScaler{}).
		Named("mmcscaler").
		Complete(r)
}
