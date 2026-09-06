package promq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type Client struct {
	api     promv1.API
	timeout time.Duration
	logger  logr.Logger
}

func New(addr string, timeout time.Duration, logger logr.Logger) (*Client, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive, got %v", timeout)
	}
	// TODO - resuse prom client across reconciliations
	promClient, err := promapi.NewClient(promapi.Config{
		Address: addr,
	})
	if err != nil {
		return nil, fmt.Errorf("prometheus client for %q: %w: %w", addr, ErrBadAddress, err)
	}

	var client = Client{
		api:     promv1.NewAPI(promClient),
		timeout: timeout,
		logger:  logger,
	}
	return &client, nil
}

func classifyErr(err error) error {
	var apiErr *promv1.Error
	if !errors.As(err, &apiErr) {
		return ErrPromUnavailable
	}

	switch apiErr.Type {
	case promv1.ErrBadData, promv1.ErrExec, promv1.ErrClient: // 4xx
		return ErrBadQuery

	case promv1.ErrBadResponse:
		return ErrBadResponse

	case promv1.ErrTimeout: // 503
		return ErrQueryTimeout

	default: // ErrCancelled & ErrServer
		return ErrPromUnavailable
	}
}

func (c *Client) QueryVector(ctx context.Context, q string, ts time.Time) (model.Vector, error) {
	var errClientTimeout = errors.New("prometheus client deadline exceeded")
	promctx, cancel := context.WithTimeoutCause(ctx, c.timeout, errClientTimeout)
	defer cancel()

	result, warnings, err := c.api.Query(promctx, q, ts, promv1.WithTimeout(c.timeout+2*time.Second))

	if err != nil {
		// parnet context died - controller shutting down.
		if ctx.Err() != nil {
			return nil, fmt.Errorf("query %q: %w", q, context.Cause(ctx))
		}

		// promctx died? likely never received prom reply
		if cause := context.Cause(promctx); cause != nil {
			return nil, fmt.Errorf("query %q: %w: %w", q, ErrPromUnavailable, cause)
		}

		// received an error reply from prometheus
		return nil, fmt.Errorf("query %q: %w: %w", q, classifyErr(err), err)
	}
	for _, w := range warnings {
		// Fail if return with warnings?
		c.logger.Info("Prometheus returned with warnings", "warning", w)
	}
	vecResult, ok := result.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("query %q: %w: got %s, want vector", q, ErrBadResult, result.Type())
	}
	if vecResult.Len() == 0 {
		return nil, ErrNoData
	}
	return vecResult, nil
}
