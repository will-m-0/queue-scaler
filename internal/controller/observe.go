package controller

import (
	"errors"

	"github.com/will-m-0/queue-scaler/internal/promq"
)

type obsFailure int

const (
	obsOK obsFailure = iota
	obsIncomplete
	obsMisconfigured // will not fix itself without a spec or image change
)

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
