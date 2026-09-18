package nats

import natsio "github.com/nats-io/nats.go"

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
