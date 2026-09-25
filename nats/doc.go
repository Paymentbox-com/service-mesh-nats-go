// Package nats implements the mesh contract over NATS using nats.go.
//
// # Targets
//
// Segments are joined with "." to form a subject. A segment must be
// non-empty and free of ".", "*", ">", whitespace, and non-printable
// characters. Targets are literal; wildcards are not supported. A target that
// breaks these rules yields mesh.ErrInvalidTarget.
//
// # Configuration
//
// A mesh.Config is parsed at construction. NewClient reads the connection
// keys URLKey, NameKey, ConnectTimeoutKey, and RequestTimeoutKey. New reads
// mesh.DeploymentGroupKey, which is required, and ConcurrencyKey. Each
// constructor ignores the other's keys, so one Config can be given to both.
// A value that does not parse yields ErrBadConfig. A logger is passed to New
// as WithLogger.
//
// # Metadata
//
// Message metadata travels as NATS headers, one value per key. The runtime
// reads no message keys. It writes HandlerErrorHeader on a reply when an
// endpoint handler fails.
//
// Binding metadata is read at construction. mesh.ConsumerGroupKey on an
// Endpoint or Subscriber, or on its Target, selects the queue group. The
// binding's metadata wins over the target's. mesh.DeploymentGroupKey on a
// client-side Target is ignored; NATS chooses the group on the receiving
// side.
//
// # Delivery
//
// A consumer group is a NATS queue group. Every binding joins the group named
// by mesh.DeploymentGroupKey unless mesh.ConsumerGroupKey overrides it.
// mesh.ConsumerGroupNone gives a plain subscription: every instance receives
// every message, and for an endpoint every instance replies. Any other value
// names the group. Two bindings on one subject are two subscriptions, with
// whatever delivery NATS gives them.
//
// # Handler failure
//
// An endpoint handler that returns an error or panics produces an empty reply
// with HandlerErrorHeader set to the error text, and the requester receives a
// *HandlerError. A request with no reply subject is handled and its result
// discarded. A failing subscriber handler is logged.
//
// # Concurrency
//
// Handlers run on their own goroutines, at most ConcurrencyKey at once across
// all bindings. Beyond that, deliveries wait in nats.go's pending buffer.
//
// # Lifecycle
//
// A Client owns a NATS connection. NewClient returns a connected client, and
// Close closes that connection; Close is idempotent. Request and Publish
// after Close return ErrClosed.
//
// A Runtime is built from a Client the application constructed, and that
// client is the runtime's connection. Runtime.Client returns it in every
// state. Start subscribes every binding on the client's connection and
// flushes; on a closed client it returns ErrClosed. Stop unsubscribes, waits
// for in-flight handlers until its context is done, cancels the handlers'
// context, flushes, and closes the client. It returns ctx.Err() when
// handlers were abandoned. Start after Stop returns ErrStopped. Closing the
// client directly ends the runtime's connection; Stop afterwards returns the
// drain result.
//
// # Service map
//
// NewClient takes a mesh.ServiceMap. The client holds it and returns it from
// ServiceMap, and a runtime's ServiceMap is its client's. Nothing here
// validates a target against it.
//
// # Errors
//
// Transport errors from nats.go are returned unchanged, most often
// nats.ErrNoResponders when nothing serves a target and nats.ErrTimeout when
// the request timeout fires.
package nats
