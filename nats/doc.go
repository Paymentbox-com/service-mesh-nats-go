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
// A mesh.Config is parsed at construction. Beyond mesh.DeploymentGroupKey the
// keys are URLKey, NameKey, ConnectTimeoutKey, RequestTimeoutKey, and
// ConcurrencyKey. A value that does not parse yields ErrBadConfig. Values
// that cannot be strings, the logger and extra nats.Options, are passed as
// Option arguments.
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
// Close closes that connection; Close is idempotent. Request and Publish on
// a client that has not connected return ErrNotConnected, and after Close
// they return ErrClosed.
//
// A Runtime holds one Client, available from Client in every state, that
// owns the runtime's connection. Start connects it, subscribes every binding
// on its connection, and flushes. Stop unsubscribes, waits for in-flight
// handlers until its context is done, cancels the handlers' context,
// flushes, and closes the client. It returns ctx.Err() when handlers were
// abandoned. Start after Stop returns ErrStopped. Closing the runtime's
// client directly ends the runtime's connection; Stop afterwards returns the
// drain result.
//
// # Service map
//
// New and NewClient each take a mesh.ServiceMap. The runtime and the client
// hold it and return it from ServiceMap; a client from Runtime.Client returns
// the runtime's. Nothing here validates a target against it.
//
// # Errors
//
// Transport errors from nats.go are returned unchanged, most often
// nats.ErrNoResponders when nothing serves a target and nats.ErrTimeout when
// the request timeout fires.
package nats
