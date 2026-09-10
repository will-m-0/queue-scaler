package promq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-logr/logr"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type Client struct {
	addr    string
	api     promv1.API
	timeout time.Duration
	logger  logr.Logger
}

func (c *Client) Addr() string { return c.addr }

func New(addr string, timeout time.Duration, logger logr.Logger) (*Client, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive, got %v", timeout)
	}
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
		addr:    addr,
	}
	return &client, nil
}

func (c *Client) QueryScalar(ctx context.Context, q string, ts time.Time) (float64, error) {
	vec, err := c.QueryVector(ctx, q, ts)
	if err != nil {
		return 0, err
	}
	if len(vec) != 1 {
		return 0, fmt.Errorf("query: %q: %w: got %d samples, expected 1", q, ErrBadResult, len(vec))
	}
	val := float64(vec[0].Value)
	if math.IsNaN(val) {
		// 0 / 0
		return 0, fmt.Errorf("query: %q: %w: got NaN", q, ErrNoData)
	}
	if math.IsInf(val, 0) {
		return 0, fmt.Errorf("query: %q: %w: got infinite result to query", q, ErrBadResult)
	}
	return val, nil
}

func (c *Client) QueryVector(ctx context.Context, q string, ts time.Time) (model.Vector, error) {
	var errClientTimeout = errors.New("prometheus client deadline exceeded")
	promctx, cancel := context.WithTimeoutCause(ctx, c.timeout+2*time.Second, errClientTimeout)
	defer cancel()

	result, warnings, err := c.api.Query(promctx, q, ts, promv1.WithTimeout(c.timeout))

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
