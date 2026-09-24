package nats

import (
	"errors"

	natsio "github.com/nats-io/nats.go"
)

// Errors this package defines beyond the three in the mesh contract.
var (
	// ErrBadConfig is returned by New and NewClient when a configuration
	// value, or a per-call option, cannot be parsed. It is wrapped with the
	// key and value.
	ErrBadConfig = errors.New("nats: bad configuration value")

	// ErrClosed is returned by Request and Publish after Close, and by Start
	// on a runtime whose client is closed. A runtime's Stop closes its
	// client.
	ErrClosed = errors.New("nats: client is closed")

	// ErrAlreadyStarted is returned by Start on a running runtime.
	ErrAlreadyStarted = errors.New("nats: runtime already started")

	// ErrStopped is returned by Start after Stop. A runtime is not
	// restartable.
	ErrStopped = errors.New("nats: runtime has been stopped")
)

// HandlerError is returned by Request when the serving handler returned an
// error or panicked. Text is the handler's error message as sent by the
// serving runtime.
type HandlerError struct {
	Text string
}

func (e *HandlerError) Error() string {
	return "nats: handler failed: " + e.Text
}

// HandlerErrorHeader is the reply header this runtime sets when an endpoint
// handler fails. Application metadata should not use this key.
const HandlerErrorHeader = "Mesh-Handler-Error"

// Transport errors callers most often test for, re-exported so that a caller
// need not import nats.go. They are the same values, so errors.Is works.
var (
	ErrNoResponders = natsio.ErrNoResponders
	ErrTimeout      = natsio.ErrTimeout
)
