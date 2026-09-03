package promq

import "errors"

var (
	// permanent failures - the query or config is wrong.
	ErrBadAddress = errors.New("bad prometheus address")
	ErrBadQuery   = errors.New("prometheus rejected query")
	ErrBadResult  = errors.New("unexpected result type")

	// transient failures - retry on the next reconcile.
	ErrPromUnavailable = errors.New("prometheus unavailable")
	ErrNoData          = errors.New("query returned no samples")
)
