// Package mesh is the Go form of the Service Mesh API Specification. It
// fixes the core types, the handler signatures, the Client and Runtime
// interfaces, the transport-agnostic configuration keys, and three errors.
// Each transport lives in its own package, implements these interfaces, and
// exports its own New and NewClient.
//
// The runtime moves bytes. Encoding and decoding a payload belong to the
// layer above this package. A runtime never inspects, decodes, or transforms
// a payload.
package mesh

import (
	"context"
	"errors"
)

// Kind says what a Target is.
type Kind int

const (
	// KindRoute targets reply to each request with a Message.
	KindRoute Kind = iota
	// KindTopic targets do not reply.
	KindTopic
)

// String returns the kind's name for error messages and logs.
func (k Kind) String() string {
	switch k {
	case KindRoute:
		return "route"
	case KindTopic:
		return "topic"
	default:
		return "unknown"
	}
}

// Target identifies a receiving channel on the mesh. Segments are assembled
// into a transport-specific address by the runtime; the assembled form never
// leaves the runtime. Metadata holds transport-specific configuration for
// this target, such as ConsumerGroupKey or, on the client side where a
// transport needs it, DeploymentGroupKey.
type Target struct {
	Segments []string
	Kind     Kind
	Metadata map[string]string
}

// Equal reports whether two targets name the same channel: same segments
// and same kind. Metadata is configuration, not identity.
func (t Target) Equal(o Target) bool {
	if t.Kind != o.Kind || len(t.Segments) != len(o.Segments) {
		return false
	}
	for i := range t.Segments {
		if t.Segments[i] != o.Segments[i] {
			return false
		}
	}
	return true
}

// ServiceMap lists every Target available on the mesh. How it is built and
// how discovery works is up to the consumer.
type ServiceMap struct {
	Targets []Target
}

// Message is what travels between services.
//
// Payload is owned by the receiver of the call it is passed to. The runtime
// does not retain or mutate it after the call. nil and an empty slice are
// both an empty payload.
type Message struct {
	Target   Target
	Metadata map[string]string
	Payload  []byte
}

// EndpointHandler answers a request. The returned Message is sent back to
// the requester; its Target is ignored because the runtime already knows
// the reply destination. The context is cancelled when the runtime's drain
// budget expires during Stop.
type EndpointHandler func(context.Context, Message) (Message, error)

// SubscriberHandler consumes a published message. The context is cancelled
// when the runtime's drain budget expires during Stop.
type SubscriberHandler func(context.Context, Message) error

// Endpoint pairs a KindRoute target with a handler that replies. Metadata
// holds transport-specific settings for the binding.
type Endpoint struct {
	Target   Target
	Metadata map[string]string
	Handler  EndpointHandler
}

// Subscriber pairs a KindTopic target with a handler that does not reply.
// Metadata holds transport-specific settings for the binding.
type Subscriber struct {
	Target   Target
	Metadata map[string]string
	Handler  SubscriberHandler
}

// Config carries configuration as string keys and values. The specification
// defines DeploymentGroupKey; every other key belongs to a transport.
type Config map[string]string

// Configuration keys the specification defines. Everything else is
// transport-specific and documented by the transport package.
const (
	// DeploymentGroupKey names the logical group a service belongs to. It is
	// required in the Config given to a Runtime. A transport that needs it on
	// the client side reads it from Target.Metadata.
	DeploymentGroupKey = "deployment_group"

	// ConsumerGroupKey overrides the logical group an Endpoint or Subscriber
	// joins. By default both use the deployment group. A transport reads it
	// from the binding's Metadata or the Target's Metadata, as it documents.
	// ConsumerGroupNone means no group: every instance handles every message.
	// Any other value names the group, and one handler in that group handles
	// each message.
	ConsumerGroupKey = "consumer_group"

	// ConsumerGroupNone is the ConsumerGroupKey value that requests no group.
	ConsumerGroupNone = "none"
)

// Client is the access point for sending. It is obtained from a Runtime with
// Client, or built directly with a transport package's NewClient for a
// process that only calls.
type Client interface {
	// Request sends msg to a KindRoute target and returns the reply. opts
	// carries transport-specific options for this call and may be nil. A
	// target of the wrong kind returns ErrKindMismatch; one the transport
	// cannot address returns ErrInvalidTarget. Transport errors are returned
	// unchanged.
	Request(ctx context.Context, msg Message, opts map[string]string) (Message, error)

	// Publish sends msg to a KindTopic target. Errors are as for Request.
	Publish(ctx context.Context, msg Message, opts map[string]string) error

	// Close releases the client's connection.
	Close() error
}

// Runtime is the service process for handling requests.
type Runtime interface {
	// Client returns a client sharing this runtime's connection.
	Client() Client

	// Start connects, binds every Endpoint and Subscriber, and begins
	// receiving.
	Start(ctx context.Context) error

	// Stop stops receiving, waits for in-flight handlers until ctx is done,
	// then closes. Handlers still running when ctx is done are abandoned.
	// The specification's drain duration, a float of seconds, is expressed
	// here as ctx's deadline: context.WithTimeout(ctx, drain).
	Stop(ctx context.Context) error

	// Running reports whether Start has succeeded and Stop has not run.
	Running() bool
}

// ErrKindMismatch is returned when a Target's kind does not match its use:
// Request with a topic, Publish with a route, an Endpoint on a topic, or a
// Subscriber on a route.
var ErrKindMismatch = errors.New("mesh: target kind does not match its use")

// ErrInvalidTarget is returned when a Target's segments cannot be carried by
// the transport.
var ErrInvalidTarget = errors.New("mesh: target cannot be carried by this transport")

// ErrNoDeploymentGroup is returned by a runtime constructor when the Config
// has no DeploymentGroupKey or its value is empty.
var ErrNoDeploymentGroup = errors.New("mesh: config deployment_group is required")
