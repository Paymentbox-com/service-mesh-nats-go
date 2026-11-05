package nats

import (
	"context"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// openReceivers runs the boot check, then subscribes each core endpoint and
// subscriber and consumes each JetStream subscriber. It runs inside Start,
// under r.mu. On failure it returns what it opened so far with the error.
func (r *Runtime) openReceivers(ctx context.Context, nc *natsio.Conn, js jetstream.JetStream) ([]*natsio.Subscription, []*jsConsumer, error) {
	if err := checkEndpointStreams(ctx, js, r.subscriptions); err != nil {
		return nil, nil, err
	}

	subs := make([]*natsio.Subscription, 0, len(r.subscriptions))
	var consumers []*jsConsumer
	for i := range r.subscriptions {
		b := &r.subscriptions[i]
		if b.js != nil {
			c, err := r.consume(ctx, js, b)
			if err != nil {
				return subs, consumers, err
			}
			consumers = append(consumers, c)
			continue
		}
		sub, err := r.subscribe(nc, b)
		if err != nil {
			return subs, consumers, err
		}
		subs = append(subs, sub)
	}
	return subs, consumers, nil
}

// subscribe makes a core subscription for b, plain or in its queue group.
// It must run under r.mu. Its handlers get the handler context of the Start
// that calls it, copied here so they never read r.handlerCtx themselves.
func (r *Runtime) subscribe(nc *natsio.Conn, b *subscription) (*natsio.Subscription, error) {
	hctx := r.handlerCtx
	cb := func(m *natsio.Msg) {
		arrival := time.Now()
		r.dispatch(hctx, func() { r.serve(hctx, nc, b, m, arrival) })
	}
	if b.queue == "" {
		return nc.Subscribe(b.subject, cb)
	}
	return nc.QueueSubscribe(b.subject, b.queue, cb)
}

// waitSubscriptionsDrained returns once every subscription has delivered its
// buffered messages and closed, or the connection has closed. A subscription
// on a connection that closed before the drain began is already invalid.
func waitSubscriptionsDrained(nc *natsio.Conn, subs []*natsio.Subscription) {
	closed := nc.StatusChanged(natsio.CLOSED)
	defer nc.RemoveStatusListener(closed)
	for _, s := range subs {
		if !s.IsValid() {
			continue
		}
		select {
		case <-s.StatusChanged(natsio.SubscriptionClosed):
		case <-closed:
			return
		}
	}
}

// abortStart undoes a failed Start. It unsubscribes, stops the consumers,
// cancels the handlers' context, and stops accepting.
func (r *Runtime) abortStart(subs []*natsio.Subscription, consumers []*jsConsumer) {
	for _, s := range subs {
		_ = s.Unsubscribe()
	}
	for _, c := range consumers {
		c.cc.Stop()
	}
	r.cancelHandlers()
	r.setAccepting(false)
}
