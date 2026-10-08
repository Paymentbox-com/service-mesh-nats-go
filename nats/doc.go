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
// mesh.DeploymentGroupKey, which is required, ConcurrencyKey, and
// MaxDeliverKey. Each
// constructor ignores the other's keys, so one Config can be given to both.
// A value that does not parse yields ErrBadConfig. A logger is passed to New
// as WithLogger.
//
// # Metadata
//
// Message metadata travels as NATS headers, one value per key. The transport
// uses these keys the specification defines:
//
//   - mesh.HandlerErrorKey on a reply when an endpoint handler fails.
//   - mesh.TimeoutKey on each request, in whole milliseconds until the
//     request's bound.
//   - mesh.DeadlineKey on a request that carries mesh.TimeoutKey, set to its
//     arrival time plus the timeout, and on a JetStream delivery from an
//     acking consumer, set to when the server sends it again. The endpoint
//     or subscriber handler's context carries the same deadline.
//   - mesh.DeliveryAttemptKey on a JetStream delivery.
//   - mesh.MessageIDKey, which is one field with Nats-Msg-Id. On a sent
//     message mesh.MessageIDKey wins, and on a message handed to a handler
//     Nats-Msg-Id wins.
//
// A JetStream delivery also carries the Mesh-Nats- keys, such as
// StreamMetadataKey.
//
// Endpoint and Subscriber metadata is read at construction.
// mesh.ConsumerGroupKey in it selects the queue group, and StreamKey and the
// other JetStream options configure a JetStream subscriber. A Target carries
// no consumer group or deployment group, so a Target's metadata is not read
// for either.
//
// # Delivery
//
// A consumer group is a NATS queue group. Every endpoint and subscriber joins
// the group named by the runtime's mesh.DeploymentGroupKey unless its own
// mesh.ConsumerGroupKey overrides it.
// mesh.ConsumerGroupNone gives a plain subscription: every instance receives
// every message, and for an endpoint every instance replies. Any other value
// names the group. Two endpoints or subscribers on one subject are two
// subscriptions, with
// whatever delivery NATS gives them.
//
// # JetStream
//
// JetStream applies to topic targets only. A Publish with StreamKey in its
// options sends a JetStream publish and waits for the stream's ack. A
// Subscriber with StreamKey consumes from a pull consumer on that stream
// instead of a queue subscription. Its consumer group names the consumer, and
// mesh.ConsumerGroupNone gives each instance its own consumer. Start creates
// or updates the consumers, and streams are created outside the runtime.
// docs/jetstream.md in the repository describes it in full.
//
// # Handler failure
//
// An endpoint handler that returns an error or panics produces an empty reply
// with mesh.HandlerErrorKey set to the error text, and the requester receives
// a *HandlerError. A request with no reply subject is handled and its result
// discarded. A failing subscriber handler is logged, and a JetStream
// subscriber's message is nacked unless its AckPolicyKey is "none".
//
// # Concurrency
//
// Handlers run on their own goroutines, at most ConcurrencyKey at once across
// all endpoints and subscribers. Beyond that, deliveries wait in nats.go's pending buffer.
//
// # Lifecycle
//
// A Client owns a NATS connection. NewClient returns a connected client, and
// Close closes that connection; Close is idempotent. Request and Publish
// after Close return ErrClosed.
//
// A Runtime is built from a Client the application constructed, and that
// client is the runtime's connection. Runtime.Client returns it in every
// state. Start subscribes every endpoint and subscriber on the client's connection and
// flushes; on a closed client it returns ErrClosed. Stop unsubscribes, waits
// for in-flight handlers until its context is done, cancels the handlers'
// context, flushes, and closes the client. It returns ctx.Err() when
// handlers were abandoned, after nacking every JetStream message not yet
// settled. Start after Stop returns ErrStopped. Closing the
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
