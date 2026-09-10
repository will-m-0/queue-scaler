package promq

import (
	"errors"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

var (
	ErrBadAddress      = errors.New("bad prometheus address")
	ErrBadQuery        = errors.New("prometheus rejected query")
	ErrBadResult       = errors.New("unexpected result type")
	ErrBadResponse     = errors.New("malformed prometheus response")
	ErrPromUnavailable = errors.New("prometheus unavailable")
	ErrQueryTimeout    = errors.New("prometheus query execution timeout")
	ErrNoData          = errors.New("query returned no samples")
)

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
