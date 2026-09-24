package nats

import (
	"context"
	"errors"
	"sync"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// Client implements mesh.Client over a NATS connection it owns. A client is
// unconnected, connected, or closed. NewClient returns a connected client. A
// Runtime builds an unconnected client, connects it in Start, and closes it
// in Stop.
type Client struct {
	settings   settings
	serviceMap mesh.ServiceMap

	mu     sync.Mutex // guards nc and closed
	nc     *natsio.Conn
	closed bool
}

var _ mesh.Client = (*Client)(nil)

// NewClient connects to NATS and returns a client that owns the connection
// and holds serviceMap. mesh.DeploymentGroupKey is ignored.
func NewClient(cfg mesh.Config, serviceMap mesh.ServiceMap, opts ...Option) (*Client, error) {
	s, err := parseSettings(cfg, opts, false)
	if err != nil {
		return nil, err
	}
	c := newClient(s, serviceMap)
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// newClient returns an unconnected client.
func newClient(s settings, serviceMap mesh.ServiceMap) *Client {
	return &Client{settings: s, serviceMap: serviceMap}
}

// connect opens the connection. On a connected client it returns nil; after
// Close it returns ErrClosed.
func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.nc != nil {
		return nil
	}
	nc, err := c.settings.connect()
	if err != nil {
		return err
	}
	c.nc = nc
	return nil
}

// ServiceMap returns the map this client was built with: the one given to
// NewClient, or the runtime's for a client from Runtime.Client. The client
// does not otherwise use it.
func (c *Client) ServiceMap() mesh.ServiceMap {
	return c.serviceMap
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
	subj, err := subject(msg.Target)
	if err != nil {
		return mesh.Message{}, err
	}
	timeout, err := durationSetting(opts, RequestTimeoutKey, c.settings.requestTimeout)
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
	subj, err := subject(msg.Target)
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

// Close closes the connection when one is open and marks the client closed.
// It is idempotent and always returns nil. For a client from Runtime.Client
// this ends the runtime's connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
	}
	return nil
}

// connection returns the open connection, ErrClosed after Close, or
// ErrNotConnected before connect.
func (c *Client) connection() (*natsio.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed:
		return nil, ErrClosed
	case c.nc == nil:
		return nil, ErrNotConnected
	}
	return c.nc, nil
}
