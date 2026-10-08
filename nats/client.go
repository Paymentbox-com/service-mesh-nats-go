package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// Client implements mesh.Client over a NATS connection it owns. NewClient
// returns a connected client and Close closes it. A Runtime is built from a
// Client and subscribes on its connection; the runtime's Stop closes it.
type Client struct {
	requestTimeout time.Duration
	serviceMap     mesh.ServiceMap

	mu     sync.Mutex // guards nc, js, and closed
	nc     *natsio.Conn
	js     jetstream.JetStream
	closed bool
}

// errRequestTimeout is the cause of a context bounded by the request
// timeout, so its expiry can be told apart from the caller's deadline.
var errRequestTimeout = errors.New("nats: request timeout")

var _ mesh.Client = (*Client)(nil)

// NewClient connects to NATS and returns a client that owns the connection
// and holds serviceMap. cfg is read for URLKey, NameKey, ConnectTimeoutKey,
// and RequestTimeoutKey; every other key is ignored. A value that does not
// parse returns ErrBadConfig; a connection failure returns the nats.go error
// unchanged.
func NewClient(cfg mesh.Config, serviceMap mesh.ServiceMap) (*Client, error) {
	s, err := parseClientSettings(cfg)
	if err != nil {
		return nil, err
	}
	nc, err := s.connect()
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Client{requestTimeout: s.requestTimeout, serviceMap: serviceMap, nc: nc, js: js}, nil
}

// ServiceMap returns the map given to NewClient. The client does not
// otherwise use it.
func (c *Client) ServiceMap() mesh.ServiceMap {
	return c.serviceMap
}

// Request sends msg to a route target and returns the reply.
//
// The request is bounded by the request timeout or ctx's deadline, whichever
// comes first. opts may carry RequestTimeoutKey to override the configured
// request timeout for this call, and StreamKey returns ErrBadConfig. Expiry
// of the request timeout is reported as natsio.ErrTimeout, and a deadline or
// cancellation on ctx as ctx's own error.
//
// The request carries mesh.TimeoutKey, set to the time left before that
// bound in whole milliseconds. mesh.MessageIDKey is copied to Nats-Msg-Id.
func (c *Client) Request(ctx context.Context, msg mesh.Message, opts map[string]string) (mesh.Message, error) {
	if err := checkKind(msg.Target, mesh.KindRoute, "Request"); err != nil {
		return mesh.Message{}, err
	}
	subj, err := subject(msg.Target)
	if err != nil {
		return mesh.Message{}, err
	}
	if v := opts[StreamKey]; v != "" {
		return mesh.Message{}, fmt.Errorf("%w: %s=%q applies to topic targets only", ErrBadConfig, StreamKey, v)
	}
	timeout, err := durationSetting(opts, RequestTimeoutKey, c.requestTimeout)
	if err != nil {
		return mesh.Message{}, err
	}
	nc, err := c.connection()
	if err != nil {
		return mesh.Message{}, err
	}

	ctx, cancel, wait := boundContext(ctx, timeout)
	defer cancel()

	h := outgoingHeader(msg.Metadata)
	if h == nil {
		h = natsio.Header{}
	}
	h.Set(mesh.TimeoutKey, strconv.FormatInt(wait.Milliseconds(), 10))

	reply, err := nc.RequestMsgWithContext(ctx, &natsio.Msg{
		Subject: subj,
		Header:  h,
		Data:    msg.Payload,
	})
	if err != nil {
		return mesh.Message{}, timeoutError(ctx, err)
	}

	if text, failed := reply.Header[mesh.HandlerErrorKey]; failed {
		he := &HandlerError{}
		if len(text) > 0 {
			he.Text = text[0]
		}
		return mesh.Message{}, he
	}

	return mesh.Message{
		Target:   msg.Target,
		Metadata: fromHeader(reply.Header),
		Payload:  reply.Data,
	}, nil
}

// Publish sends msg to a topic target. mesh.MessageIDKey is copied to
// Nats-Msg-Id.
//
// When opts carries StreamKey, Publish sends a JetStream publish that
// expects that stream and waits for its ack, bounded like Request. opts may
// then carry RequestTimeoutKey. The ack's error or the nats.go error is
// returned unchanged, except that expiry of the request timeout is reported
// as natsio.ErrTimeout.
func (c *Client) Publish(ctx context.Context, msg mesh.Message, opts map[string]string) error {
	if err := checkKind(msg.Target, mesh.KindTopic, "Publish"); err != nil {
		return err
	}
	subj, err := subject(msg.Target)
	if err != nil {
		return err
	}
	stream := opts[StreamKey]
	timeout := c.requestTimeout
	if stream != "" {
		if timeout, err = durationSetting(opts, RequestTimeoutKey, timeout); err != nil {
			return err
		}
	}
	nc, js, err := c.connections()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := &natsio.Msg{
		Subject: subj,
		Header:  outgoingHeader(msg.Metadata),
		Data:    msg.Payload,
	}
	if stream == "" {
		return nc.PublishMsg(m)
	}

	ctx, cancel, _ := boundContext(ctx, timeout)
	defer cancel()
	if _, err := js.PublishMsg(ctx, m, jetstream.WithExpectStream(stream)); err != nil {
		return timeoutError(ctx, err)
	}
	return nil
}

// boundContext bounds ctx by timeout and returns how long the result has
// left, which is the shorter of timeout and ctx's own time.
func boundContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, time.Duration) {
	wait := timeout
	if d, ok := ctx.Deadline(); ok {
		wait = min(wait, max(time.Until(d), 0))
	}
	bounded, cancel := context.WithTimeoutCause(ctx, timeout, errRequestTimeout)
	return bounded, cancel, wait
}

// timeoutError reports err from a call on a context made by boundContext as
// natsio.ErrTimeout when the request timeout expired, and unchanged
// otherwise.
func timeoutError(ctx context.Context, err error) error {
	if errors.Is(err, context.DeadlineExceeded) && errors.Is(context.Cause(ctx), errRequestTimeout) {
		return natsio.ErrTimeout
	}
	return err
}

// Close closes the connection and marks the client closed. It is idempotent
// and always returns nil. For a client a Runtime was built from, this ends
// the runtime's connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.nc.Close()
	}
	return nil
}

// connection returns the open connection, or ErrClosed after Close.
func (c *Client) connection() (*natsio.Conn, error) {
	nc, _, err := c.connections()
	return nc, err
}

// connections returns the open connection and its JetStream context, or
// ErrClosed after Close.
func (c *Client) connections() (*natsio.Conn, jetstream.JetStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, ErrClosed
	}
	return c.nc, c.js, nil
}
