package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// flushBudget bounds the flush in Start and Stop when ctx has no deadline.
const flushBudget = 5 * time.Second

type runtimeState int

const (
	stateCreated runtimeState = iota
	stateRunning
	stateStopped
)

// subscription is a validated Endpoint or Subscriber, ready to subscribe.
// queue is the NATS queue group, and empty means a plain subscription. js is
// set for a Subscriber that consumes from a JetStream stream.
type subscription struct {
	subject    string
	queue      string
	target     mesh.Target
	endpoint   mesh.EndpointHandler
	subscriber mesh.SubscriberHandler
	js         *jsSettings
}

// Runtime implements mesh.Runtime over NATS. It is built from a Client and
// serves its endpoints and subscribers on that client's connection.
type Runtime struct {
	client        *Client
	logger        *slog.Logger
	subscriptions []subscription

	mu        sync.Mutex // guards state, subs, consumers, and the lifecycle methods
	state     runtimeState
	subs      []*natsio.Subscription
	consumers []*jsConsumer

	running atomic.Bool

	// accepting and jsAccepting gate new handler goroutines for core
	// subscriptions and JetStream consumers, so no callback can add to wg
	// once Stop has begun waiting.
	acceptMu    sync.RWMutex
	accepting   bool
	jsAccepting bool

	// pending holds the deliveries from acking consumers that are not yet
	// settled, so Stop can nak them when its drain ends.
	pendingMu sync.Mutex
	pending   map[*delivery]struct{}

	handlerCtx     context.Context
	cancelHandlers context.CancelFunc
	sem            chan struct{}
	wg             sync.WaitGroup
}

var _ mesh.Runtime = (*Runtime)(nil)

// New validates cfg and every endpoint and subscriber, and returns a runtime
// that serves them on client's connection. cfg is read for
// mesh.DeploymentGroupKey, which is required, ConcurrencyKey, and
// MaxDeliverKey. Connection keys are ignored because client already carries
// them. WithLogger sets the logger.
//
// Endpoint metadata is read for mesh.ConsumerGroupKey, and StreamKey on an
// endpoint returns ErrBadConfig. Subscriber metadata is read for
// mesh.ConsumerGroupKey and the JetStream options.
//
// The parameter is a *Client, not a mesh.Client, because the runtime
// subscribes through the NATS connection the client owns.
//
// New returns mesh.ErrNoDeploymentGroup, ErrBadConfig, mesh.ErrKindMismatch,
// or mesh.ErrInvalidTarget. Two endpoints or subscribers that assemble to
// the same subject become two subscriptions.
func New(client *Client, cfg mesh.Config, endpoints []mesh.Endpoint, subscribers []mesh.Subscriber, opts ...Option) (*Runtime, error) {
	if client == nil {
		return nil, errors.New("nats: client is nil")
	}
	s, err := parseRuntimeSettings(cfg, opts)
	if err != nil {
		return nil, err
	}
	r := &Runtime{
		client:  client,
		logger:  s.logger,
		sem:     make(chan struct{}, s.concurrency),
		pending: map[*delivery]struct{}{},
	}

	prepare := func(t mesh.Target, want mesh.Kind, use string, md map[string]string) (subscription, error) {
		if err := checkKind(t, want, use); err != nil {
			return subscription{}, err
		}
		subj, err := subject(t)
		if err != nil {
			return subscription{}, err
		}
		return subscription{
			subject: subj,
			queue:   consumerGroup(md, s.deploymentGroup),
			target:  t,
		}, nil
	}

	for _, e := range endpoints {
		b, err := prepare(e.Target, mesh.KindRoute, "Endpoint", e.Metadata)
		if err != nil {
			return nil, err
		}
		if e.Handler == nil {
			return nil, fmt.Errorf("nats: endpoint %q has a nil handler", b.subject)
		}
		if v := e.Metadata[StreamKey]; v != "" {
			return nil, fmt.Errorf("%w: %s=%q on endpoint %q applies to subscribers only", ErrBadConfig, StreamKey, v, b.subject)
		}
		b.endpoint = e.Handler
		r.subscriptions = append(r.subscriptions, b)
	}
	for _, sub := range subscribers {
		b, err := prepare(sub.Target, mesh.KindTopic, "Subscriber", sub.Metadata)
		if err != nil {
			return nil, err
		}
		if sub.Handler == nil {
			return nil, fmt.Errorf("nats: subscriber %q has a nil handler", b.subject)
		}
		b.subscriber = sub.Handler
		if b.js, err = parseJetStream(sub.Metadata, b.subject, b.queue, s.maxDeliver); err != nil {
			return nil, err
		}
		r.subscriptions = append(r.subscriptions, b)
	}
	return r, nil
}

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

// Running reports whether Start has succeeded and Stop has not run.
func (r *Runtime) Running() bool {
	return r.running.Load()
}

// Start subscribes every endpoint and subscriber on the client's connection,
// flushes, and begins receiving. It returns ErrAlreadyStarted on a running
// runtime, ErrStopped after Stop, and ErrClosed when the client is closed.
//
// With at least one JetStream subscriber, Start first returns
// ErrStreamCoversRoute when a stream covers an endpoint subject. It then
// looks up each JetStream subscriber's stream and creates or looks up its
// consumer, returning the nats.go error unchanged when one fails.
//
// A failure removes the subscriptions and consumes made so far and leaves
// the runtime and the client as they were.
func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case stateRunning:
		return ErrAlreadyStarted
	case stateStopped:
		return ErrStopped
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	nc, js, err := r.client.connections()
	if err != nil {
		return err
	}

	r.handlerCtx, r.cancelHandlers = context.WithCancel(context.Background())
	r.setAccepting(true)

	subs := make([]*natsio.Subscription, 0, len(r.subscriptions))
	var consumers []*jsConsumer
	fail := func(err error) error {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
		for _, c := range consumers {
			c.cc.Stop()
		}
		r.cancelHandlers()
		r.setAccepting(false)
		return err
	}

	if r.hasJetStream() {
		if err := checkEndpointStreams(ctx, js, r.subscriptions); err != nil {
			return fail(err)
		}
	}

	for i := range r.subscriptions {
		b := &r.subscriptions[i]
		if b.js != nil {
			c, err := r.consume(ctx, js, b)
			if err != nil {
				return fail(err)
			}
			consumers = append(consumers, c)
			continue
		}
		cb := func(m *natsio.Msg) {
			arrival := time.Now()
			r.dispatch(false, func() { r.serve(nc, b, m, arrival) })
		}
		var sub *natsio.Subscription
		if b.queue == "" {
			sub, err = nc.Subscribe(b.subject, cb)
		} else {
			sub, err = nc.QueueSubscribe(b.subject, b.queue, cb)
		}
		if err != nil {
			return fail(err)
		}
		subs = append(subs, sub)
	}

	// Ensure the server has every subscription before Start returns.
	if err := flush(ctx, nc, flushBudget); err != nil {
		return fail(err)
	}

	r.subs = subs
	r.consumers = consumers
	r.state = stateRunning
	r.running.Store(true)
	return nil
}

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

func (r *Runtime) hasJetStream() bool {
	for _, b := range r.subscriptions {
		if b.js != nil {
			return true
		}
	}
	return false
}

// Stop stops receiving, waits for in-flight handlers until ctx is done,
// cancels the handlers' context, flushes, and closes the client. It returns
// ctx.Err() when handlers were abandoned. Stop on a runtime that is not
// running does nothing. When the client was closed directly, Stop skips the
// flush and returns the drain result.
//
// JetStream consumers stop pulling, and the messages already pulled are
// handled during the drain. When ctx is done first, every message from an
// acking consumer that is not yet settled is nacked and logged as a warning,
// whether its handler had not started or was still running.
func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != stateRunning {
		return nil
	}
	r.state = stateStopped
	r.running.Store(false)

	for _, s := range r.subs {
		_ = s.Unsubscribe()
	}
	r.acceptMu.Lock()
	r.accepting = false
	r.acceptMu.Unlock()
	consumers := r.consumers
	for _, c := range consumers {
		c.cc.Drain()
	}

	done := make(chan struct{})
	go func() {
		for _, c := range consumers {
			<-c.cc.Closed()
		}
		r.setAccepting(false)
		r.wg.Wait()
		close(done)
	}()

	var drainErr error
	select {
	case <-done:
	case <-ctx.Done():
		drainErr = ctx.Err()
	}
	r.cancelHandlers()

	if drainErr != nil {
		r.setAccepting(false)
		r.abandonPending()
		// Messages still buffered reach the callbacks, which nak them.
		waitConsumersClosed(consumers, flushBudget)
	}

	if nc, err := r.client.connection(); err == nil && drainErr == nil {
		if err := flush(ctx, nc, flushBudget); err != nil {
			r.logger.Warn("nats: flush during stop failed", "error", err)
		}
	}

	_ = r.client.Close()
	r.subs = nil
	r.consumers = nil
	return drainErr
}

func (r *Runtime) isAccepting(jetStream bool) bool {
	r.acceptMu.RLock()
	defer r.acceptMu.RUnlock()
	return r.acceptingLocked(jetStream)
}

func (r *Runtime) acceptingLocked(jetStream bool) bool {
	if jetStream {
		return r.jsAccepting
	}
	return r.accepting
}

func (r *Runtime) setAccepting(on bool) {
	r.acceptMu.Lock()
	r.accepting = on
	r.jsAccepting = on
	r.acceptMu.Unlock()
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

// dispatch runs fn on its own goroutine, bounded by the concurrency
// semaphore and tracked for drain. It reports false, and does not run fn,
// for a message arriving after its kind of subscription stopped accepting.
func (r *Runtime) dispatch(jetStream bool, fn func()) bool {
	if !r.isAccepting(jetStream) {
		return false
	}
	select {
	case r.sem <- struct{}{}:
	case <-r.handlerCtx.Done():
		return false
	}

	// Checked again under the lock, so no handler is added to wg once Stop
	// has begun waiting.
	r.acceptMu.RLock()
	if !r.acceptingLocked(jetStream) {
		r.acceptMu.RUnlock()
		<-r.sem
		return false
	}
	r.wg.Add(1)
	r.acceptMu.RUnlock()

	go func() {
		defer r.wg.Done()
		defer func() { <-r.sem }()
		fn()
	}()
	return true
}

// serve handles a message from a core subscription. arrival is when it
// reached the callback.
func (r *Runtime) serve(nc *natsio.Conn, b *subscription, m *natsio.Msg, arrival time.Time) {
	in := mesh.Message{Target: b.target, Metadata: incomingMetadata(m.Header), Payload: m.Data}

	if b.subscriber != nil {
		if err := callSubscriber(r.handlerCtx, b.subscriber, in); err != nil {
			r.logger.Error("nats: subscriber handler failed", "subject", b.subject, "error", err)
		}
		return
	}

	ctx := r.handlerCtx
	if v, ok := in.Metadata[mesh.TimeoutKey]; ok {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms >= 0 {
			deadline := arrival.Add(time.Duration(ms) * time.Millisecond)
			in.Metadata[mesh.DeadlineKey] = deadline.Format(time.RFC3339Nano)
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
	}

	out, err := callEndpoint(ctx, b.endpoint, in)
	if err != nil {
		r.logger.Error("nats: endpoint handler failed", "subject", b.subject, "error", err)
	}
	if m.Reply == "" {
		return
	}

	reply := &natsio.Msg{Subject: m.Reply}
	if err != nil {
		reply.Header = natsio.Header{}
		reply.Header.Set(mesh.HandlerErrorKey, err.Error())
	} else {
		reply.Header = toHeader(out.Metadata)
		reply.Data = out.Payload
	}
	if err := nc.PublishMsg(reply); err != nil {
		r.logger.Error("nats: reply failed", "subject", b.subject, "error", err)
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

func callEndpoint(ctx context.Context, h mesh.EndpointHandler, in mesh.Message) (out mesh.Message, err error) {
	defer func() {
		if p := recover(); p != nil {
			out, err = mesh.Message{}, fmt.Errorf("panic: %v", p)
		}
	}()
	return h(ctx, in)
}

func callSubscriber(ctx context.Context, h mesh.SubscriberHandler, in mesh.Message) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return h(ctx, in)
}

// flush round-trips to the server. nats.go requires a deadline, so fallback
// applies when ctx has none.
func flush(ctx context.Context, nc *natsio.Conn, fallback time.Duration) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, fallback)
		defer cancel()
	}
	return nc.FlushWithContext(ctx)
}
