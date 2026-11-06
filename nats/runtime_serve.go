package nats

import (
	"context"
	"fmt"
	"strconv"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// serve handles a message from a core subscription. hctx is the handler
// context of the Start that opened the subscription, and arrival is when the
// message reached the callback.
func (r *Runtime) serve(hctx context.Context, nc *natsio.Conn, b *subscription, m *natsio.Msg, arrival time.Time) {
	in := mesh.Message{Target: b.target, Metadata: incomingMetadata(m.Header), Payload: m.Data}

	if b.subscriber != nil {
		if err := callSubscriber(hctx, b.subscriber, in); err != nil {
			r.logger.Error("nats: subscriber handler failed", "subject", b.subject, "error", err)
		}
		return
	}

	ctx := hctx
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
