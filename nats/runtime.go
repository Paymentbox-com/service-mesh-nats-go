package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"

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

	// accepting gates new handler goroutines, so no callback can add to wg
	// once Stop has begun waiting.
	acceptMu  sync.RWMutex
	accepting bool

	// pending holds the deliveries from acking consumers that are not yet
	// settled, so Stop can nak them when its drain ends.
	pendingMu sync.Mutex
	pending   map[*delivery]struct{}

	// missing holds the JetStream consumers nats.go reported missing, as
	// *jsConsumer keys, so watchMissing can log them again.
	missing sync.Map

	handlerCtx     context.Context
	cancelHandlers context.CancelFunc
	sem            chan struct{}
	wg             sync.WaitGroup
}

var _ mesh.Runtime = (*Runtime)(nil)

// New validates cfg and every endpoint and subscriber, and returns a runtime
// that serves them on client's connection. cfg is read for
// mesh.DeploymentGroupKey, which is required, ConcurrencyKey, MaxDeliverKey,
// and the pull keys. Connection keys are ignored because client already
// carries them. WithLogger sets the logger.
//
// The consumer group comes from each Endpoint's and Subscriber's
// ConsumerGroup. Their settings are their Metadata over their Target's
// Metadata. StreamKey in an endpoint's settings returns ErrBadConfig, and a
// subscriber's settings are read for the JetStream options.
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

	for _, e := range endpoints {
		b, err := prepareSubscription(e.Target, mesh.KindRoute, "Endpoint", e.ConsumerGroup, s.deploymentGroup)
		if err != nil {
			return nil, err
		}
		if e.Handler == nil {
			return nil, fmt.Errorf("nats: endpoint %q has a nil handler", b.subject)
		}
		if v := mergeSettings(e.Target.Metadata, e.Metadata)[StreamKey]; v != "" {
			return nil, fmt.Errorf("%w: %s=%q on endpoint %q applies to subscribers only", ErrBadConfig, StreamKey, v, b.subject)
		}
		b.endpoint = e.Handler
		r.subscriptions = append(r.subscriptions, b)
	}
	for _, sub := range subscribers {
		b, err := prepareSubscription(sub.Target, mesh.KindTopic, "Subscriber", sub.ConsumerGroup, s.deploymentGroup)
		if err != nil {
			return nil, err
		}
		if sub.Handler == nil {
			return nil, fmt.Errorf("nats: subscriber %q has a nil handler", b.subject)
		}
		b.subscriber = sub.Handler
		md := mergeSettings(sub.Target.Metadata, sub.Metadata)
		if b.js, err = parseJetStream(md, b.subject, b.queue, s.maxDeliver, s.pull); err != nil {
			return nil, err
		}
		r.subscriptions = append(r.subscriptions, b)
	}
	return r, nil
}

// Running reports whether Start has succeeded and Stop has not run.
func (r *Runtime) Running() bool {
	return r.running.Load()
}

// Start subscribes every endpoint and subscriber on the client's connection,
// flushes, and begins receiving. It returns ErrAlreadyStarted on a running
// runtime, ErrStopped after Stop, and ErrClosed when the client is closed.
//
// Start first returns ErrStreamCoversRoute when a stream covers an endpoint
// subject. A server without JetStream passes this check. It then looks up
// each JetStream subscriber's stream, creates or looks up its consumer, and
// starts pulling from it, returning the nats.go error unchanged when one
// fails. A pull setting outside nats.go's limits fails here.
//
// A failure unsubscribes and stops the consumers opened so far, and leaves
// the runtime and the client as they were.
func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.checkStartable(ctx); err != nil {
		return err
	}
	nc, js, err := r.client.connections()
	if err != nil {
		return err
	}

	r.handlerCtx, r.cancelHandlers = context.WithCancel(context.Background())
	r.setAccepting(true)

	subs, consumers, err := r.openReceivers(ctx, nc, js)
	if err == nil {
		// Ensure the server has every subscription before Start returns.
		err = flush(ctx, nc, flushBudget)
	}
	if err != nil {
		r.abortStart(subs, consumers)
		return err
	}

	r.subs = subs
	r.consumers = consumers
	if len(consumers) > 0 {
		go r.watchMissing(r.handlerCtx)
	}
	r.state = stateRunning
	r.running.Store(true)
	return nil
}

// checkStartable returns ErrAlreadyStarted on a running runtime, ErrStopped
// after Stop, and ctx's error when ctx is done.
func (r *Runtime) checkStartable(ctx context.Context) error {
	switch r.state {
	case stateRunning:
		return ErrAlreadyStarted
	case stateStopped:
		return ErrStopped
	}
	return ctx.Err()
}

// Stop drains every subscription and consumer, waits for in-flight handlers
// until ctx is done, cancels the handlers' context, flushes, and closes the
// client. It returns ctx.Err() when handlers were abandoned. Stop on a
// runtime that is not running does nothing. When the client was closed
// directly, Stop skips the flush and returns the drain result.
//
// Draining stops new deliveries and hands the messages already received to
// their handlers. When ctx is done first, every message from an acking
// consumer that is not yet settled is nacked and logged as a warning,
// whether its handler had not started or was still running. Core messages
// still buffered then are dropped.
func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != stateRunning {
		return nil
	}
	r.state = stateStopped
	r.running.Store(false)

	subs, consumers := r.subs, r.consumers
	for _, s := range subs {
		_ = s.Drain()
	}
	for _, c := range consumers {
		c.cc.Drain()
	}

	done := make(chan struct{})
	go func() {
		waitSubscriptionsDrained(r.client.nc, subs)
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
		r.abandonDrain(consumers)
	} else {
		r.flushOnStop(ctx)
	}

	_ = r.client.Close()
	r.subs = nil
	r.consumers = nil
	return drainErr
}

// flushOnStop flushes the client's connection so the server has every reply
// and ack, unless the client was closed directly.
func (r *Runtime) flushOnStop(ctx context.Context) {
	nc, err := r.client.connection()
	if err != nil {
		return
	}
	if err := flush(ctx, nc, flushBudget); err != nil {
		r.logger.Warn("nats: flush during stop failed", "error", err)
	}
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
