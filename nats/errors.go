package nats

import (
	"errors"

	natsio "github.com/nats-io/nats.go"
)

// Errors this runtime defines beyond the three in the mesh contract.
var (
	// ErrBadConfig is returned by New and NewClient when a configuration
	// value, or a per-call option, cannot be parsed. It is wrapped with the
	// key and value.
	ErrBadConfig = errors.New("nats: bad configuration value")

	// ErrNotConnected is returned by Request and Publish on a client that
	// has no connection yet: a runtime's client before Start has succeeded.
	ErrNotConnected = errors.New("nats: client is not connected")

	// ErrClosed is returned by Request and Publish after Close. A runtime's
	// client is closed by Stop.
	ErrClosed = errors.New("nats: client is closed")

	// ErrAlreadyStarted is returned by Start on a running runtime.
	ErrAlreadyStarted = errors.New("nats: runtime already started")

	// ErrStopped is returned by Start after Stop. A runtime is not
	// restartable.
	ErrStopped = errors.New("nats: runtime has been stopped")

	// ErrDuplicateTarget is returned by New when two endpoints, two
	// subscribers, or an endpoint and a subscriber assemble to the same
	// subject.
	ErrDuplicateTarget = errors.New("nats: duplicate target")
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
