package nats

import (
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// toHeader copies metadata into a NATS header, one value per key.
func toHeader(md map[string]string) natsio.Header {
	if len(md) == 0 {
		return nil
	}
	h := make(natsio.Header, len(md))
	for k, v := range md {
		h.Set(k, v)
	}
	return h
}

// fromHeader copies a NATS header into metadata, first value per key.
func fromHeader(h natsio.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	md := make(map[string]string, len(h))
	for k, vs := range h {
		if len(vs) > 0 {
			md[k] = vs[0]
		}
	}
	return md
}

// outgoingHeader is toHeader for a message being sent. mesh.MessageIDKey and
// Nats-Msg-Id are one field, and mesh.MessageIDKey wins.
func outgoingHeader(md map[string]string) natsio.Header {
	h := toHeader(md)
	if id, ok := md[mesh.MessageIDKey]; ok {
		h.Set(jetstream.MsgIDHeader, id)
	}
	return h
}

// incomingMetadata is fromHeader for a message handed to a handler.
// Nats-Msg-Id and mesh.MessageIDKey are one field, and Nats-Msg-Id wins. The
// result is never nil.
func incomingMetadata(h natsio.Header) map[string]string {
	md := fromHeader(h)
	if md == nil {
		md = map[string]string{}
	}
	if id, ok := md[jetstream.MsgIDHeader]; ok {
		md[mesh.MessageIDKey] = id
	}
	return md
}
