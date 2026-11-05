package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// abandonDrain closes intake, naks every message not yet settled, and waits
// for messages still buffered in the consumers to reach the callbacks, which
// nak them too.
func (r *Runtime) abandonDrain(consumers []*jsConsumer) {
	r.setAccepting(false)
	r.abandonPending()
	waitConsumersClosed(consumers, flushBudget)
}

// receive is the Consume callback of a JetStream consumer.
func (r *Runtime) receive(c *jsConsumer, m jetstream.Msg) {
	d := &delivery{consumer: c, msg: m, arrival: time.Now()}
	// The message is tracked before dispatch so Stop can nak it even while
	// it waits for a handler slot. Otherwise the server holds it until its
	// ack wait runs out.
	if c.acks {
		r.pendingMu.Lock()
		r.pending[d] = struct{}{}
		r.pendingMu.Unlock()
	}
	if !r.dispatch(func() { r.serveJetStream(d) }) && c.acks {
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
			if now := time.Now(); now.After(deadline) {
				r.logLateStart(c, meta, in.Metadata[mesh.MessageIDKey], now, now.Sub(deadline))
			}
		}
	}

	herr := callSubscriber(ctx, b.subscriber, in)
	if herr != nil {
		attrs := []any{"subject", b.subject, "stream", b.js.stream}
		if meta != nil {
			attrs = append(attrs, "delivery_attempt", meta.NumDelivered)
		}
		attrs = append(attrs, "error", herr)
		r.logger.Error("nats: subscriber handler failed", attrs...)
	}
	// Without acks there is no reply to send, and Stop never sees these
	// deliveries, so they skip settle.
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
		if meta != nil && c.lastDelivery(meta.NumDelivered) {
			r.logFinalFailure(c, meta, in.Metadata[mesh.MessageIDKey])
		}
		return
	}
	if b.js.ack == ackDouble {
		// The handler's ctx may already be done, so waiting for the server's
		// confirmation gets its own timeout. The handler slot stays taken
		// until the wait ends.
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

// logLateStart logs a handler that starts lateBy after its delivery's ack
// deadline, unless the consumer logged one within lateStartLogInterval.
func (r *Runtime) logLateStart(c *jsConsumer, meta *jetstream.MsgMetadata, messageID string, now time.Time, lateBy time.Duration) {
	skipped, logged := c.lateStartLogged(now)
	if !logged {
		return
	}
	b := c.sub
	attrs := []any{
		"subject", b.subject,
		"stream", b.js.stream,
		"consumer", meta.Consumer,
		"stream_sequence", meta.Sequence.Stream,
		"delivery_attempt", meta.NumDelivered,
	}
	if messageID != "" {
		attrs = append(attrs, "message_id", messageID)
	}
	attrs = append(attrs, "late_by", lateBy, "skipped", skipped)
	r.logger.Warn("nats: handler started after its ack deadline", attrs...)
}

// logFinalFailure logs a message whose handler failed on its last delivery.
func (r *Runtime) logFinalFailure(c *jsConsumer, meta *jetstream.MsgMetadata, messageID string) {
	b := c.sub
	attrs := []any{
		"subject", b.subject,
		"stream", b.js.stream,
		"consumer", meta.Consumer,
		"stream_sequence", meta.Sequence.Stream,
		"delivery_attempt", meta.NumDelivered,
		"max_deliver", c.maxDeliver,
	}
	if messageID != "" {
		attrs = append(attrs, "message_id", messageID)
	}
	r.logger.Error("nats: message will not be delivered again", attrs...)
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
