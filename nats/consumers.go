package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// lateStartLogInterval is the least time between two late-start warnings for
// one consumer.
var lateStartLogInterval = time.Minute

// missingLogInterval is how often the runtime logs each consumer that is
// still missing.
var missingLogInterval = 5 * time.Second

// jsConsumer is a JetStream subscriber's consumer once Start has created or
// found it. ctx is the handler context of that Start, which its handlers get.
// acks, ackWait, backoff, and maxDeliver come from the server's settings for
// the consumer. A maxDeliver of -1 means no limit.
type jsConsumer struct {
	sub        *subscription
	ctx        context.Context
	consumer   jetstream.Consumer
	cc         jetstream.ConsumeContext
	acks       bool
	ackWait    time.Duration
	backoff    []time.Duration
	maxDeliver int

	// lateMu guards lastLateLog, when a late start was last logged, and
	// lateSkipped, how many late starts were not logged since then.
	lateMu      sync.Mutex
	lastLateLog time.Time
	lateSkipped int
}

// openConsumer looks up the subscriber's stream and creates or looks up its
// consumer. Errors from nats.go are returned unchanged.
func openConsumer(ctx context.Context, js jetstream.JetStream, b *subscription) (*jsConsumer, error) {
	s := b.js
	stream, err := js.Stream(ctx, s.stream)
	if err != nil {
		return nil, err
	}

	var cons jetstream.Consumer
	switch {
	case s.ordered():
		cons, err = stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
			FilterSubjects:    []string{b.subject},
			DeliverPolicy:     s.config.DeliverPolicy,
			InactiveThreshold: s.config.InactiveThreshold,
		})
	case !s.create:
		cons, err = stream.Consumer(ctx, s.group)
	case s.group == "":
		cons, err = stream.CreateConsumer(ctx, s.config)
	default:
		cons, err = stream.CreateOrUpdateConsumer(ctx, s.config)
	}
	if err != nil {
		return nil, err
	}

	c := &jsConsumer{sub: b, consumer: cons}
	if s.ordered() {
		return c, nil
	}
	cfg := cons.CachedInfo().Config
	c.acks = s.ack != ackNone && cfg.AckPolicy != jetstream.AckNonePolicy
	c.ackWait = cfg.AckWait
	c.backoff = cfg.BackOff
	c.maxDeliver = cfg.MaxDeliver
	return c, nil
}

// lateStartLogged reports whether a late start at now is logged, and if so,
// how many late starts were not logged since the last one that was.
func (c *jsConsumer) lateStartLogged(now time.Time) (skipped int, logged bool) {
	c.lateMu.Lock()
	defer c.lateMu.Unlock()
	if now.Sub(c.lastLateLog) < lateStartLogInterval {
		c.lateSkipped++
		return 0, false
	}
	skipped = c.lateSkipped
	c.lastLateLog, c.lateSkipped = now, 0
	return skipped, true
}

// deadline is when the server sends a message again unless it is acked,
// given when it arrived and which delivery it is.
func (c *jsConsumer) deadline(arrival time.Time, delivered uint64) time.Time {
	wait := c.ackWait
	if n := len(c.backoff); n > 0 {
		i := min(max(int(delivered)-1, 0), n-1)
		wait = c.backoff[i]
	}
	return arrival.Add(wait)
}

// consume opens a JetStream subscriber's consumer and starts consuming it
// with the subscriber's pull settings. From then on nats.go pulls messages
// in batches and hands each one to receive, which can happen before consume
// returns. It must run under r.mu, because it copies r.handlerCtx into the
// consumer so its handlers never read r.handlerCtx themselves.
func (r *Runtime) consume(ctx context.Context, js jetstream.JetStream, b *subscription) (*jsConsumer, error) {
	c, err := openConsumer(ctx, js, b)
	if err != nil {
		return nil, err
	}
	c.ctx = r.handlerCtx
	opts := append(b.js.pull.consumeOpts(), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		r.consumeError(c, err)
	}))
	c.cc, err = c.consumer.Consume(func(m jetstream.Msg) { r.receive(c, m) }, opts...)
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

// lastDelivery reports whether delivery number delivered is the last one the
// server sends. A maxDeliver of -1 converts to the largest uint64, so no
// delivery is the last.
func (c *jsConsumer) lastDelivery(delivered uint64) bool {
	return delivered >= uint64(c.maxDeliver)
}

// consumeError is the ConsumeErrHandler of a JetStream subscriber's consumer.
//
// nats.go reports a deleted consumer once and then stops consuming. When the
// consumer was deleted while the connection was down, nats.go keeps pulling
// after it reconnects, and each pull gets no responders. Either way the
// consumer is logged and added to r.missing.
//
// nats.go creates an ordered consumer again by itself, and calls this handler
// holding the lock CachedInfo takes, so the ordered check comes first.
func (r *Runtime) consumeError(c *jsConsumer, err error) {
	b := c.sub
	if !b.js.ordered() && (errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, natsio.ErrNoResponders)) {
		r.missing.Store(c, struct{}{})
		r.logMissing(c, "error", err)
		return
	}
	r.logger.Warn("nats: consume error", "subject", b.subject, "stream", b.js.stream, "error", err)
}

// watchMissing checks every consumer in r.missing each missingLogInterval
// until ctx is done.
func (r *Runtime) watchMissing(ctx context.Context) {
	t := time.NewTicker(missingLogInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.missing.Range(func(k, _ any) bool {
				r.checkMissing(ctx, k.(*jsConsumer))
				return true
			})
		}
	}
}

// checkMissing logs c while it is still missing, and removes it from
// r.missing once it is back. A consumer nats.go stopped consuming stays
// missing until Stop, even when it exists again, because nothing pulls from
// it. One nats.go still pulls from is back when the server finds it, and
// nats.go's next pull gets its messages. While the connection is down, c is
// left as it is.
func (r *Runtime) checkMissing(ctx context.Context, c *jsConsumer) {
	select {
	case <-c.cc.Closed():
		r.logMissing(c)
		return
	default:
	}
	if nc, _, err := r.client.connections(); err != nil || !nc.IsConnected() {
		return
	}
	lctx, cancel := context.WithTimeout(ctx, missingLogInterval)
	defer cancel()
	_, err := c.consumer.Info(lctx)
	switch {
	case err == nil:
		r.missing.Delete(c)
	case errors.Is(err, jetstream.ErrConsumerNotFound):
		r.logMissing(c)
	default:
		r.logMissing(c, "error", err)
	}
}

func (r *Runtime) logMissing(c *jsConsumer, attrs ...any) {
	b := c.sub
	attrs = append([]any{"subject", b.subject, "stream", b.js.stream, "consumer", c.consumer.CachedInfo().Name}, attrs...)
	r.logger.Error("nats: consumer missing", attrs...)
}
