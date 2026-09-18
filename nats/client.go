package nats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-nats-go/mesh"
)

// Client implements mesh.Client over a NATS connection.
type Client struct {
	nc             atomic.Pointer[natsio.Conn]
	requestTimeout time.Duration
	ownsConn       bool
	subjects       sync.Map // targetKey -> string
	closeOnce      sync.Once
}

var _ mesh.Client = (*Client)(nil)

// NewClient connects to NATS and returns a client that owns the connection.
// mesh.DeploymentGroupKey is ignored.
func NewClient(cfg mesh.Config, opts ...Option) (*Client, error) {
	s, err := parseSettings(cfg, opts, false)
	if err != nil {
		return nil, err
	}
	nc, err := s.connect()
	if err != nil {
		return nil, err
	}
	c := &Client{requestTimeout: s.requestTimeout, ownsConn: true}
	c.nc.Store(nc)
	return c, nil
}

// newSharedClient returns a client whose connection a Runtime sets and
// clears. Its Close is a no-op.
func newSharedClient(s settings) *Client {
	return &Client{requestTimeout: s.requestTimeout}
}

// Request sends msg to a route target and returns the reply.
//
// opts may carry RequestTimeoutKey to override the configured request
// timeout for this call. When ctx has no deadline, that timeout applies and
// its expiry is reported as natsio.ErrTimeout. A deadline or cancellation on
// ctx is reported as ctx's own error.
func (c *Client) Request(ctx context.Context, msg mesh.Message, opts map[string]string) (mesh.Message, error) {
	if err := checkKind(msg.Target, mesh.KindRoute, "Request"); err != nil {
		return mesh.Message{}, err
	}
	subj, err := c.subject(msg.Target)
	if err != nil {
		return mesh.Message{}, err
	}
	timeout, err := durationSetting(opts, RequestTimeoutKey, c.requestTimeout)
	if err != nil {
		return mesh.Message{}, err
	}
	nc, err := c.connection()
	if err != nil {
		return mesh.Message{}, err
	}

	ownTimeout := false
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
		ownTimeout = true
	}

	reply, err := nc.RequestMsgWithContext(ctx, &natsio.Msg{
		Subject: subj,
		Header:  toHeader(msg.Metadata),
		Data:    msg.Payload,
	})
	if err != nil {
		if ownTimeout && errors.Is(err, context.DeadlineExceeded) {
			return mesh.Message{}, natsio.ErrTimeout
		}
		return mesh.Message{}, err
	}

	if text, failed := reply.Header[HandlerErrorHeader]; failed {
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

// Publish sends msg to a topic target. opts is accepted for the interface
// and not read.
func (c *Client) Publish(ctx context.Context, msg mesh.Message, opts map[string]string) error {
	if err := checkKind(msg.Target, mesh.KindTopic, "Publish"); err != nil {
		return err
	}
	subj, err := c.subject(msg.Target)
	if err != nil {
		return err
	}
	nc, err := c.connection()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nc.PublishMsg(&natsio.Msg{
		Subject: subj,
		Header:  toHeader(msg.Metadata),
		Data:    msg.Payload,
	})
}

// Close releases the connection when this client owns it. For a client
// obtained from a Runtime it does nothing.
func (c *Client) Close() error {
	if !c.ownsConn {
		return nil
	}
	c.closeOnce.Do(func() {
		if nc := c.nc.Swap(nil); nc != nil {
			nc.Close()
		}
	})
	return nil
}

func (c *Client) connection() (*natsio.Conn, error) {
	nc := c.nc.Load()
	if nc == nil {
		return nil, ErrNotRunning
	}
	return nc, nil
}

// subject assembles and validates a target once, then serves it from cache.
func (c *Client) subject(t mesh.Target) (string, error) {
	key := targetKey(t)
	if s, ok := c.subjects.Load(key); ok {
		return s.(string), nil
	}
	s, err := subject(t)
	if err != nil {
		return "", err
	}
	c.subjects.Store(key, s)
	return s, nil
}
