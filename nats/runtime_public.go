package nats

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// ServiceMap returns the client's map. This runtime does not otherwise use
// it.
func (r *Runtime) ServiceMap() mesh.ServiceMap {
	return r.client.ServiceMap()
}

// Client returns the client this runtime was built from, in every state.
// Its Request and Publish return ErrClosed after Stop. Closing it directly
// ends the runtime's connection.
func (r *Runtime) Client() mesh.Client {
	return r.client
}

// Receiver is an endpoint or subscriber the runtime is running. It reads the
// settings the runtime uses, which are the mesh.Endpoint's or
// mesh.Subscriber's Metadata over its Target's Metadata.
type Receiver struct {
	client   *Client
	sub      *subscription
	consumer *jsConsumer
}

// Receiver returns the runtime's endpoint or subscriber for target, while the
// runtime is running. It reports false when the runtime is not running or has
// no endpoint or subscriber for target. A target's kind decides which one it
// can match, because Target.Equal compares kinds.
func (r *Runtime) Receiver(target mesh.Target) (*Receiver, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != stateRunning {
		return nil, false
	}
	for i := range r.subscriptions {
		b := &r.subscriptions[i]
		if !b.target.Equal(target) {
			continue
		}
		rc := &Receiver{client: r.client, sub: b}
		for _, c := range r.consumers {
			if c.sub == b {
				rc.consumer = c
			}
		}
		return rc, true
	}
	return nil, false
}

// Target returns the receiver's target.
func (rc *Receiver) Target() mesh.Target {
	return rc.sub.target
}

// ConsumerGroup returns the resolved consumer group: the mesh.Endpoint's or
// mesh.Subscriber's ConsumerGroup, or the runtime's deployment group when
// that is empty.
func (rc *Receiver) ConsumerGroup() string {
	if rc.sub.queue == "" {
		return mesh.ConsumerGroupNone
	}
	return rc.sub.queue
}

// Stream returns the stream named by the receiver's settings, or nil and no
// error when they have no StreamKey, which is so for every endpoint. It
// returns ErrClosed after the runtime's client is closed, and a lookup
// failure as the nats.go error unchanged.
func (rc *Receiver) Stream(ctx context.Context) (jetstream.Stream, error) {
	if rc.sub.js == nil {
		return nil, nil
	}
	_, js, err := rc.client.connections()
	if err != nil {
		return nil, err
	}
	return js.Stream(ctx, rc.sub.js.stream)
}

// Consumer returns the consumer the receiver pulls from, or nil when its
// settings have no StreamKey. For an ordered consumer, Info describes the
// consumer nats.go is using at the time of the call.
func (rc *Receiver) Consumer() jetstream.Consumer {
	if rc.consumer == nil {
		return nil
	}
	return rc.consumer.consumer
}
