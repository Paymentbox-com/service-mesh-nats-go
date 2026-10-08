package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// Delivery metadata keys this transport defines. A handler of a JetStream
// subscriber receives them along with mesh.DeliveryAttemptKey, and
// mesh.DeadlineKey when the consumer acks.
const (
	// StreamMetadataKey is the stream the message is stored on.
	StreamMetadataKey = "Mesh-Nats-Stream"

	// ConsumerMetadataKey is the consumer the message was delivered to.
	ConsumerMetadataKey = "Mesh-Nats-Consumer"

	// StreamSequenceMetadataKey is the message's sequence in the stream.
	StreamSequenceMetadataKey = "Mesh-Nats-Stream-Sequence"

	// ConsumerSequenceMetadataKey is the delivery's sequence in the consumer.
	ConsumerSequenceMetadataKey = "Mesh-Nats-Consumer-Sequence"

	// NumPendingMetadataKey is how many messages are still waiting in the
	// consumer.
	NumPendingMetadataKey = "Mesh-Nats-Num-Pending"

	// TimestampMetadataKey is when the stream stored the message, in RFC 3339.
	TimestampMetadataKey = "Mesh-Nats-Timestamp"
)

const (
	namedInactiveThreshold   = 7 * 24 * time.Hour
	orderedInactiveThreshold = 5 * time.Minute
)

type ackPolicy int

const (
	ackDouble ackPolicy = iota
	ackExplicit
	ackNone
)

// jsSettings is a Subscriber's JetStream options, read by New. group is the
// consumer name, and empty means mesh.ConsumerGroupNone.
type jsSettings struct {
	stream   string
	group    string
	create   bool
	ack      ackPolicy
	nakDelay time.Duration
	config   jetstream.ConsumerConfig
}

// ordered reports whether the subscriber uses an ordered consumer.
func (s *jsSettings) ordered() bool {
	return s.group == "" && s.ack == ackNone
}

// parseJetStream reads a Subscriber's JetStream options from md. It returns
// nil when md sets no StreamKey. maxDeliver is the runtime's default.
func parseJetStream(md map[string]string, subj, group string, maxDeliver int) (*jsSettings, error) {
	stream := md[StreamKey]
	if stream == "" {
		return nil, nil
	}
	s := &jsSettings{stream: stream, group: group, create: true}

	switch v, ok := md[CreateConsumerKey]; {
	case !ok || v == "true":
	case v == "false":
		if group == "" {
			return nil, fmt.Errorf("%w: %s=%q needs a named consumer group", ErrBadConfig, CreateConsumerKey, v)
		}
		s.create = false
	default:
		return nil, fmt.Errorf("%w: %s=%q must be \"true\" or \"false\"", ErrBadConfig, CreateConsumerKey, v)
	}

	switch v, ok := md[AckPolicyKey]; {
	case !ok || v == "double":
	case v == "explicit":
		s.ack = ackExplicit
	case v == "none":
		s.ack = ackNone
	default:
		return nil, fmt.Errorf("%w: %s=%q must be \"double\", \"explicit\", or \"none\"", ErrBadConfig, AckPolicyKey, v)
	}

	deliver := jetstream.DeliverAllPolicy
	switch v, ok := md[DeliverPolicyKey]; {
	case !ok || v == "all":
	case v == "new":
		deliver = jetstream.DeliverNewPolicy
	default:
		return nil, fmt.Errorf("%w: %s=%q must be \"all\" or \"new\"", ErrBadConfig, DeliverPolicyKey, v)
	}

	inactive := namedInactiveThreshold
	switch {
	case s.ordered():
		inactive = orderedInactiveThreshold
	case group == "":
		inactive = 0
	}

	var err error
	if s.nakDelay, err = durationSetting(md, NakDelayKey, 0); err != nil {
		return nil, err
	}
	if inactive, err = durationSetting(md, ConsumerInactiveThresholdKey, inactive); err != nil {
		return nil, err
	}
	ackWait, err := durationSetting(md, AckWaitKey, 0)
	if err != nil {
		return nil, err
	}
	if maxDeliver, err = positiveIntSetting(md, MaxDeliverKey, maxDeliver); err != nil {
		return nil, err
	}
	maxAckPending, err := positiveIntSetting(md, MaxAckPendingKey, 0)
	if err != nil {
		return nil, err
	}
	backoff, err := backoffSetting(md)
	if err != nil {
		return nil, err
	}
	if s.create && s.ack != ackNone && len(backoff) >= maxDeliver {
		return nil, fmt.Errorf("%w: %s=%d must exceed the %d entries of %s", ErrBadConfig, MaxDeliverKey, maxDeliver, len(backoff), BackoffKey)
	}

	s.config = jetstream.ConsumerConfig{
		Durable:           group,
		FilterSubject:     subj,
		DeliverPolicy:     deliver,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: inactive,
	}
	if s.ack != ackNone {
		s.config.AckPolicy = jetstream.AckExplicitPolicy
		s.config.AckWait = ackWait
		s.config.MaxDeliver = maxDeliver
		s.config.BackOff = backoff
		s.config.MaxAckPending = maxAckPending
	}
	return s, nil
}

// backoffSetting reads BackoffKey from md as a comma-separated list of
// positive Go durations.
func backoffSetting(md map[string]string) ([]time.Duration, error) {
	v, ok := md[BackoffKey]
	if !ok {
		return nil, nil
	}
	parts := strings.Split(v, ",")
	backoff := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(p))
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%w: %s=%q must be a comma-separated list of positive durations such as \"1s,5s\"", ErrBadConfig, BackoffKey, v)
		}
		backoff = append(backoff, d)
	}
	return backoff, nil
}

// jsConsumer is a JetStream subscriber's consumer once Start has created or
// found it. acks, ackWait, and backoff come from the server's settings for
// the consumer.
type jsConsumer struct {
	sub      *subscription
	consumer jetstream.Consumer
	cc       jetstream.ConsumeContext
	acks     bool
	ackWait  time.Duration
	backoff  []time.Duration
}

// openConsumer looks up the subscriber's stream and creates or looks up its
// consumer. Errors from nats.go are returned unchanged.
func openConsumer(ctx context.Context, js jetstream.JetStream, b *subscription) (*jsConsumer, error) {
	s := b.js
	stream, err := js.Stream(ctx, s.stream)
	if err != nil {
		return nil, err
	}

	var cons jetstream.Consumer
	switch {
	case s.ordered():
		cons, err = stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
			FilterSubjects:    []string{b.subject},
			DeliverPolicy:     s.config.DeliverPolicy,
			InactiveThreshold: s.config.InactiveThreshold,
		})
	case !s.create:
		cons, err = stream.Consumer(ctx, s.group)
	case s.group == "":
		cons, err = stream.CreateConsumer(ctx, s.config)
	default:
		cons, err = stream.CreateOrUpdateConsumer(ctx, s.config)
	}
	if err != nil {
		return nil, err
	}

	c := &jsConsumer{sub: b, consumer: cons}
	if s.ordered() {
		return c, nil
	}
	cfg := cons.CachedInfo().Config
	c.acks = s.ack != ackNone && cfg.AckPolicy != jetstream.AckNonePolicy
	c.ackWait = cfg.AckWait
	c.backoff = cfg.BackOff
	return c, nil
}

// deadline is when the server sends a message again unless it is acked,
// given when it arrived and which delivery it is.
func (c *jsConsumer) deadline(arrival time.Time, delivered uint64) time.Time {
	wait := c.ackWait
	if n := len(c.backoff); n > 0 {
		i := min(max(int(delivered)-1, 0), n-1)
		wait = c.backoff[i]
	}
	return arrival.Add(wait)
}

// checkEndpointStreams returns ErrStreamCoversRoute when a stream covers an
// endpoint subject.
func checkEndpointStreams(ctx context.Context, js jetstream.JetStream, subs []subscription) error {
	for _, b := range subs {
		if b.endpoint == nil {
			continue
		}
		name, err := js.StreamNameBySubject(ctx, b.subject)
		switch {
		case err == nil:
			return fmt.Errorf("%w: subject %q, stream %q", ErrStreamCoversRoute, b.subject, name)
		case errors.Is(err, jetstream.ErrStreamNotFound):
		default:
			return err
		}
	}
	return nil
}

type deliveryState int

const (
	deliveryWaiting deliveryState = iota
	deliveryRunning
	deliverySettled
)

// delivery is a JetStream message from an acking consumer, tracked from
// arrival until it is settled by its handler's result or abandoned by Stop.
type delivery struct {
	consumer *jsConsumer
	msg      jetstream.Msg
	arrival  time.Time

	mu    sync.Mutex
	state deliveryState
}

// begin marks the handler running, and reports false when Stop already
// abandoned the delivery.
func (d *delivery) begin() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == deliverySettled {
		return false
	}
	d.state = deliveryRunning
	return true
}

// settle marks the delivery settled, and reports false when it already was.
// running reports whether its handler had begun.
func (d *delivery) settle() (running, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == deliverySettled {
		return false, false
	}
	running = d.state == deliveryRunning
	d.state = deliverySettled
	return running, true
}

// deliveryMetadata adds the delivery details of a JetStream message to md.
func deliveryMetadata(md map[string]string, meta *jetstream.MsgMetadata) {
	md[mesh.DeliveryAttemptKey] = strconv.FormatUint(meta.NumDelivered, 10)
	md[StreamMetadataKey] = meta.Stream
	md[ConsumerMetadataKey] = meta.Consumer
	md[StreamSequenceMetadataKey] = strconv.FormatUint(meta.Sequence.Stream, 10)
	md[ConsumerSequenceMetadataKey] = strconv.FormatUint(meta.Sequence.Consumer, 10)
	md[NumPendingMetadataKey] = strconv.FormatUint(meta.NumPending, 10)
	md[TimestampMetadataKey] = meta.Timestamp.Format(time.RFC3339Nano)
}
