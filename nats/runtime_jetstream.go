package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// consume opens a JetStream subscriber's consumer and begins consuming it
// with nats.go's default pull settings.
func (r *Runtime) consume(ctx context.Context, js jetstream.JetStream, b *subscription) (*jsConsumer, error) {
	c, err := openConsumer(ctx, js, b)
	if err != nil {
		return nil, err
	}
	c.cc, err = c.consumer.Consume(
		func(m jetstream.Msg) { r.receive(c, m) },
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			r.logger.Warn("nats: consume error", "subject", b.subject, "stream", b.js.stream, "error", err)
		}),
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// waitConsumersClosed waits until every consumer has stopped delivering, or
// budget passes.
func waitConsumersClosed(consumers []*jsConsumer, budget time.Duration) {
	timeout := time.After(budget)
	for _, c := range consumers {
		select {
		case <-c.cc.Closed():
		case <-timeout:
			return
		}
	}
}

// receive is the Consume callback of a JetStream consumer.
func (r *Runtime) receive(c *jsConsumer, m jetstream.Msg) {
	d := &delivery{consumer: c, msg: m, arrival: time.Now()}
	if c.acks {
		r.pendingMu.Lock()
		r.pending[d] = struct{}{}
		r.pendingMu.Unlock()
	}
	if !r.dispatch(true, func() { r.serveJetStream(d) }) && c.acks {
		r.abandon(d)
	}
}

// serveJetStream handles a JetStream message and settles it by its result.
func (r *Runtime) serveJetStream(d *delivery) {
	c, b := d.consumer, d.consumer.sub
	if c.acks && !d.begin() {
		return
	}

	in := mesh.Message{Target: b.target, Metadata: incomingMetadata(d.msg.Headers()), Payload: d.msg.Data()}
	ctx := r.handlerCtx
	meta, err := d.msg.Metadata()
	if err != nil {
		r.logger.Warn("nats: message has no JetStream metadata", "subject", b.subject, "error", err)
	} else {
		deliveryMetadata(in.Metadata, meta)
		if c.acks {
			deadline := c.deadline(d.arrival, meta.NumDelivered)
			in.Metadata[mesh.DeadlineKey] = deadline.Format(time.RFC3339Nano)
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
	}

	herr := callSubscriber(ctx, b.subscriber, in)
	if herr != nil {
		r.logger.Error("nats: subscriber handler failed", "subject", b.subject, "stream", b.js.stream, "error", herr)
	}
	if !c.acks {
		return
	}
	if _, ok := d.settle(); !ok {
		return
	}
	r.untrack(d)

	if herr != nil {
		if err := r.nak(d); err != nil {
			r.logger.Error("nats: nak failed", "subject", b.subject, "stream", b.js.stream, "error", err)
		}
		return
	}
	if b.js.ack == ackDouble {
		actx, cancel := context.WithTimeout(context.Background(), r.client.requestTimeout)
		defer cancel()
		err = d.msg.DoubleAck(actx)
	} else {
		err = d.msg.Ack()
	}
	if err != nil {
		r.logger.Error("nats: ack failed", "subject", b.subject, "stream", b.js.stream, "error", err)
	}
}

func (r *Runtime) nak(d *delivery) error {
	if delay := d.consumer.sub.js.nakDelay; delay > 0 {
		return d.msg.NakWithDelay(delay)
	}
	return d.msg.Nak()
}

func (r *Runtime) untrack(d *delivery) {
	r.pendingMu.Lock()
	delete(r.pending, d)
	r.pendingMu.Unlock()
}

// abandonPending abandons every delivery not yet settled.
func (r *Runtime) abandonPending() {
	r.pendingMu.Lock()
	ds := make([]*delivery, 0, len(r.pending))
	for d := range r.pending {
		ds = append(ds, d)
	}
	r.pendingMu.Unlock()
	for _, d := range ds {
		r.abandon(d)
	}
}

// abandon naks a delivery Stop gives up on and logs it, unless it is already
// settled.
func (r *Runtime) abandon(d *delivery) {
	running, ok := d.settle()
	if !ok {
		return
	}
	r.untrack(d)
	b := d.consumer.sub
	attrs := []any{
		"subject", b.subject,
		"stream", b.js.stream,
	}
	if meta, err := d.msg.Metadata(); err == nil {
		attrs = append(attrs, "consumer", meta.Consumer, "stream_sequence", meta.Sequence.Stream)
	}
	if id := incomingMetadata(d.msg.Headers())[mesh.MessageIDKey]; id != "" {
		attrs = append(attrs, "message_id", id)
	}
	attrs = append(attrs, "handler_running", running)
	if err := r.nak(d); err != nil {
		attrs = append(attrs, "error", err)
	}
	r.logger.Warn("nats: nacked a message at the end of the drain", attrs...)
}
