package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-logr/logr"
	"github.com/will-m-0/queue-scaler/api/v1alpha1"
	"github.com/will-m-0/queue-scaler/internal/promq"
	"github.com/will-m-0/queue-scaler/internal/queue"
	appsv1 "k8s.io/api/apps/v1"
)

type obsFailure int

const (
	obsOK obsFailure = iota
	obsIncomplete
	obsMisconfigured // will not fix itself without a spec or image change
)

type observer struct {
	prom   *promq.Client
	queue  *queue.Queue
	scaler v1alpha1.MMcScaler
}

type observation struct {
	At            time.Time
	QueueLength   int64
	TotalArrivals int64
	Workers       int32
	ArrivalRate   float64
	ServiceRate   float64
}

func (r *MMcScalerReconciler) observerFor(scaler v1alpha1.MMcScaler) *observer {
	qk := scaler.Spec.QueueKey
	ak := scaler.Spec.ArrivalsKey
	return &observer{
		prom:   r.prom,
		queue:  queue.New(r.redis, qk, ak, 2*time.Second),
		scaler: scaler,
	}
}

func (o *observer) observe(ctx context.Context, deploy appsv1.Deployment, ts time.Time) (*observation, obsFailure, error) {
	smoothingWindow := o.scaler.Spec.PrometheusSmoothingWindow.Duration

	var errs []error
	check := func(source string, err error) {
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", source, err))
		}
	}

	avgServiceSeconds, err := o.prom.QueryScalar(ctx, avgServiceTimeQuery(smoothingWindow), ts)
	check("prometheus avg service time", err)

	avgArrivalRate, err := o.prom.QueryScalar(ctx, avgArrivedJobsRate(smoothingWindow), ts)
	check("prometheus avg arrival rate", err)

	// TODO - measure in transaction & check time we retrieve this is within a bound from ts, otherwise invalidate
	queueDepth, err := o.queue.Depth(ctx)
	check("redis queue depth", err)

	totalArrivals, err := o.queue.ArrivalsTotal(ctx)
	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("redis total arrivals count", "count", totalArrivals)
	check("redis total arrivals", err)

	if len(errs) > 0 {
		worst := obsIncomplete
		for _, e := range errs {
			worst = max(worst, classifyObservation(e))
		}
		return nil, worst, errors.Join(errs...)
	}

	if math.IsNaN(avgServiceSeconds) || avgServiceSeconds <= 0 {
		return nil, obsIncomplete, fmt.Errorf("avg service time %v not usable", avgServiceSeconds)
	}

	if prev := o.scaler.Status.Observation; prev != nil {
		dt := ts.Sub(prev.ObservedAt.Time).Seconds()
		delta := totalArrivals - prev.ArrivalsTotal
		if dt > 0 && delta >= 0 {
			avgArrivalRate = max(avgArrivalRate, float64(delta)/dt)
		}
	}

	return &observation{
		At:            ts,
		QueueLength:   queueDepth,
		TotalArrivals: totalArrivals,
		Workers:       deploy.Status.ReadyReplicas * o.scaler.Spec.WorkersPerPod,
		ServiceRate:   1 / avgServiceSeconds,
		ArrivalRate:   avgArrivalRate,
	}, obsOK, nil
}

func classifyObservation(err error) obsFailure {
	switch {
	case err == nil:
		return obsOK
	case errors.Is(err, promq.ErrBadQuery),
		errors.Is(err, promq.ErrBadResult),
		errors.Is(err, promq.ErrBadResponse),
		errors.Is(err, promq.ErrBadAddress):
		return obsMisconfigured
	default:
		return obsIncomplete
	}
}
