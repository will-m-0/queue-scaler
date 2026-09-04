package promq

import "errors"

var (
	ErrBadAddress      = errors.New("bad prometheus address")
	ErrBadQuery        = errors.New("prometheus rejected query")
	ErrBadResult       = errors.New("unexpected result type")
	ErrBadResponse     = errors.New("malformed prometheus response")
	ErrPromUnavailable = errors.New("prometheus unavailable")
	ErrQueryTimeout    = errors.New("prometheus query execution timeout")
	ErrNoData          = errors.New("query returned no samples")
)
