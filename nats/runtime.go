package nats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// flushBudget bounds the flush during Stop when ctx has no deadline.
const flushBudget = 5 * time.Second

type runtimeState int

const (
	stateCreated runtimeState = iota
	stateRunning
	stateStopped
)

// binding is a validated Endpoint or Subscriber. queue is the NATS queue
// group; empty means a plain subscription.
type binding struct {
	subject    string
	queue      string
	target     mesh.Target
	endpoint   mesh.EndpointHandler
	subscriber mesh.SubscriberHandler
}

// Runtime implements mesh.Runtime over NATS.
type Runtime struct {
	settings   settings
	serviceMap mesh.ServiceMap
	bindings   []binding
	client     *Client

	mu    sync.Mutex // guards state, subs, and the lifecycle methods
	state runtimeState
	subs  []*natsio.Subscription

	running atomic.Bool

	// accepting gates new handler goroutines so no callback can add to wg
	// once Stop has begun waiting.
	acceptMu  sync.RWMutex
	accepting bool

	handlerCtx     context.Context
	cancelHandlers context.CancelFunc
	sem            chan struct{}
	wg             sync.WaitGroup
}

var _ mesh.Runtime = (*Runtime)(nil)

// New validates cfg and every binding and returns a runtime that is not yet
// connected. It returns mesh.ErrNoDeploymentGroup, ErrBadConfig,
// mesh.ErrKindMismatch, mesh.ErrInvalidTarget, or ErrDuplicateTarget.
func New(cfg mesh.Config, serviceMap mesh.ServiceMap, endpoints []mesh.Endpoint, subscribers []mesh.Subscriber, opts ...Option) (*Runtime, error) {
	s, err := parseSettings(cfg, opts, true)
	if err != nil {
		return nil, err
	}
	r := &Runtime{
		settings:   s,
		serviceMap: serviceMap,
		client:     newClient(s, serviceMap),
		sem:        make(chan struct{}, s.concurrency),
	}

	seen := make(map[string]struct{}, len(endpoints)+len(subscribers))
	bind := func(t mesh.Target, want mesh.Kind, use string, md map[string]string) (binding, error) {
		if err := checkKind(t, want, use); err != nil {
			return binding{}, err
		}
		subj, err := subject(t)
		if err != nil {
			return binding{}, err
		}
		if _, dup := seen[subj]; dup {
			return binding{}, fmt.Errorf("%w: %q", ErrDuplicateTarget, subj)
		}
		seen[subj] = struct{}{}
		return binding{
			subject: subj,
			queue:   consumerGroup(md, t.Metadata, s.deploymentGroup),
			target:  t,
		}, nil
	}

	for _, e := range endpoints {
		b, err := bind(e.Target, mesh.KindRoute, "Endpoint", e.Metadata)
		if err != nil {
			return nil, err
		}
		if e.Handler == nil {
			return nil, fmt.Errorf("nats: endpoint %q has a nil handler", b.subject)
		}
		b.endpoint = e.Handler
		r.bindings = append(r.bindings, b)
	}
	for _, sub := range subscribers {
		b, err := bind(sub.Target, mesh.KindTopic, "Subscriber", sub.Metadata)
		if err != nil {
			return nil, err
		}
		if sub.Handler == nil {
			return nil, fmt.Errorf("nats: subscriber %q has a nil handler", b.subject)
		}
		b.subscriber = sub.Handler
		r.bindings = append(r.bindings, b)
	}
	return r, nil
}

// ServiceMap returns the map given to New. This runtime does not otherwise
// use it.
func (r *Runtime) ServiceMap() mesh.ServiceMap {
	return r.serviceMap
}

// Client returns the client that owns this runtime's connection. It is the
// same client in every state. Its Request and Publish return ErrNotConnected
// before Start has succeeded and ErrClosed after Stop. Closing it directly
// ends the runtime's connection.
func (r *Runtime) Client() mesh.Client {
	return r.client
}

// Running reports whether Start has succeeded and Stop has not run.
func (r *Runtime) Running() bool {
	return r.running.Load()
}

// Start connects the runtime's client, subscribes every binding on its
// connection, and begins receiving. It returns ErrAlreadyStarted on a running
// runtime and ErrStopped after Stop. A connection failure leaves the runtime
// as it was. A subscribe or flush failure closes the client, so a later Start
// returns ErrClosed.
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

	if err := r.client.connect(); err != nil {
		return err
	}
	nc, err := r.client.connection()
	if err != nil {
		return err
	}

	r.handlerCtx, r.cancelHandlers = context.WithCancel(context.Background())
	r.acceptMu.Lock()
	r.accepting = true
	r.acceptMu.Unlock()

	subs := make([]*natsio.Subscription, 0, len(r.bindings))
	fail := func(err error) error {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
		r.cancelHandlers()
		_ = r.client.Close()
		return err
	}

	for _, b := range r.bindings {
		b := b
		cb := func(m *natsio.Msg) {
			r.dispatch(func() { r.serve(nc, b, m) })
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
	if err := flush(ctx, nc, r.settings.connectTimeout); err != nil {
		return fail(err)
	}

	r.subs = subs
	r.state = stateRunning
	r.running.Store(true)
	return nil
}

// Stop stops receiving, waits for in-flight handlers until ctx is done,
// cancels the handlers' context, flushes, and closes the runtime's client.
// It returns ctx.Err() when handlers were abandoned. Stop on a runtime that
// is not running does nothing. When the client was closed directly, Stop
// skips the flush and returns the drain result.
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

	done := make(chan struct{})
	go func() {
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

	if nc, err := r.client.connection(); err == nil && drainErr == nil {
		if err := flush(ctx, nc, flushBudget); err != nil {
			r.settings.logger.Warn("nats: flush during stop failed", "error", err)
		}
	}

	_ = r.client.Close()
	r.subs = nil
	return drainErr
}

// dispatch runs fn on its own goroutine, bounded by the concurrency
// semaphore and tracked for drain. Messages arriving after Stop has begun
// are dropped.
func (r *Runtime) dispatch(fn func()) {
	r.acceptMu.RLock()
	defer r.acceptMu.RUnlock()
	if !r.accepting {
		return
	}

	select {
	case r.sem <- struct{}{}:
	case <-r.handlerCtx.Done():
		return
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() { <-r.sem }()
		fn()
	}()
}

func (r *Runtime) serve(nc *natsio.Conn, b binding, m *natsio.Msg) {
	in := mesh.Message{Target: b.target, Metadata: fromHeader(m.Header), Payload: m.Data}

	if b.subscriber != nil {
		if err := callSubscriber(r.handlerCtx, b.subscriber, in); err != nil {
			r.settings.logger.Error("nats: subscriber handler failed", "subject", b.subject, "error", err)
		}
		return
	}

	out, err := callEndpoint(r.handlerCtx, b.endpoint, in)
	if err != nil {
		r.settings.logger.Error("nats: endpoint handler failed", "subject", b.subject, "error", err)
	}
	if m.Reply == "" {
		return
	}

	reply := &natsio.Msg{Subject: m.Reply}
	if err != nil {
		reply.Header = natsio.Header{}
		reply.Header.Set(HandlerErrorHeader, err.Error())
	} else {
		reply.Header = toHeader(out.Metadata)
		reply.Data = out.Payload
	}
	if err := nc.PublishMsg(reply); err != nil {
		r.settings.logger.Error("nats: reply failed", "subject", b.subject, "error", err)
	}
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
