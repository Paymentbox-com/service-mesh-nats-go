package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// Client implements mesh.Client over a NATS connection it owns. NewClient
// returns a connected client and Close closes it. A Runtime is built from a
// Client and subscribes on its connection; the runtime's Stop closes it.
type Client struct {
	requestTimeout time.Duration
	serviceMap     mesh.ServiceMap

	mu     sync.Mutex // guards nc and closed
	nc     *natsio.Conn
	closed bool
}

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
	return &Client{requestTimeout: s.requestTimeout, serviceMap: serviceMap, nc: nc}, nil
}

// ServiceMap returns the map given to NewClient. The client does not
// otherwise use it.
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	return c.nc, nil
}
