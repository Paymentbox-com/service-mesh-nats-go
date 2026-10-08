package nats

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

func TestPublish_StoredInStream(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	c := testClient(t, testConfig(url))

	if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, map[string]string{StreamKey: "EVENTS"}); err != nil {
		t.Fatal(err)
	}
	if n := streamMessages(t, stream); n != 1 {
		t.Fatalf("stream holds %d messages, want 1", n)
	}
}

func TestPublish_NoStream(t *testing.T) {
	url := startServer(t)
	c := testClient(t, testConfig(url))

	err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, map[string]string{StreamKey: "EVENTS"})
	if !errors.Is(err, jetstream.ErrNoStreamResponse) {
		t.Fatalf("want jetstream.ErrNoStreamResponse, got %v", err)
	}
}

func TestPublish_SubjectInDifferentStream(t *testing.T) {
	url := startServer(t)
	other := createStream(t, url, "OTHER", "test.event")
	c := testClient(t, testConfig(url))

	err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, map[string]string{StreamKey: "EVENTS"})
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != 10060 {
		t.Fatalf("want the stream-not-match API error, got %v", err)
	}
	if n := streamMessages(t, other); n != 0 {
		t.Fatalf("other stream holds %d messages, want 0", n)
	}
}

func TestPublish_SameMessageIDStoredOnce(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	c := testClient(t, testConfig(url))

	msg := mesh.Message{Target: eventTarget, Metadata: map[string]string{mesh.MessageIDKey: "order-42"}}
	for i := 0; i < 2; i++ {
		if err := c.Publish(context.Background(), msg, map[string]string{StreamKey: "EVENTS"}); err != nil {
			t.Fatal(err)
		}
	}
	if n := streamMessages(t, stream); n != 1 {
		t.Fatalf("stream holds %d messages, want 1", n)
	}
}

func TestStart_MissingStream(t *testing.T) {
	url := startServer(t)
	r := newRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: map[string]string{StreamKey: "EVENTS"}, Handler: okSubscriber}})

	if err := r.Start(context.Background()); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("want jetstream.ErrStreamNotFound, got %v", err)
	}
	if r.Running() {
		t.Fatal("Running after failed Start")
	}
}

func TestStart_NamedGroupConsumerCreatedThenUpdated(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", AckWaitKey: "10s"}

	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: okSubscriber}})
	cons, err := stream.Consumer(context.Background(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	if got := cons.CachedInfo().Config; got.AckWait != 10*time.Second || got.InactiveThreshold != 7*24*time.Hour {
		t.Fatalf("created consumer settings %+v", got)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	md[AckWaitKey] = "20s"
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: okSubscriber}})
	cons, err = stream.Consumer(context.Background(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	if got := cons.CachedInfo().Config.AckWait; got != 20*time.Second {
		t.Fatalf("updated ack wait %s, want 20s", got)
	}
}

func TestStart_CreateConsumerFalseUsesExistingConsumer(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	_, err := stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       "audit",
		FilterSubject: "test.event",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan mesh.Message, 1)
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", CreateConsumerKey: "false", AckWaitKey: "10s"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		got <- m
		return nil
	}}})

	c := testClient(t, testConfig(url))
	if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
	cons, err := stream.Consumer(context.Background(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	if ackWait := cons.CachedInfo().Config.AckWait; ackWait != time.Minute {
		t.Fatalf("consumer ack wait %s, want the existing 1m", ackWait)
	}
}

func TestStart_CreateConsumerFalseMissingConsumer(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", CreateConsumerKey: "false"}
	r := newRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: okSubscriber}})

	if err := r.Start(context.Background()); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("want jetstream.ErrConsumerNotFound, got %v", err)
	}
}

func TestStart_NoneSubscribersGetTheirOwnConsumers(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: mesh.ConsumerGroupNone}
	for i := 0; i < 2; i++ {
		startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: okSubscriber}})
	}

	info, err := stream.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Consumers != 2 {
		t.Fatalf("stream has %d consumers, want 2", info.State.Consumers)
	}
}

// countingSubscriber counts deliveries per stream sequence.
type countingSubscriber struct {
	mu   sync.Mutex
	seen map[string]int
}

func (s *countingSubscriber) handle(ctx context.Context, m mesh.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = map[string]int{}
	}
	s.seen[m.Metadata[StreamSequenceMetadataKey]]++
	return nil
}

func (s *countingSubscriber) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.seen {
		n += c
	}
	return n
}

// publishEvents publishes n messages to eventTarget.
func publishEvents(t *testing.T, url string, n int) {
	t.Helper()
	c := testClient(t, testConfig(url))
	for i := 0; i < n; i++ {
		if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, map[string]string{StreamKey: "EVENTS"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConsume_NamedGroupInstancesSplitMessages(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit"}
	var a, b countingSubscriber
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: a.handle}})
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: b.handle}})

	publishEvents(t, url, 10)
	waitFor(t, func() bool { return a.total()+b.total() >= 10 })
	time.Sleep(100 * time.Millisecond) // give an unwanted extra delivery time to show up
	if n := a.total() + b.total(); n != 10 {
		t.Fatalf("group handled %d deliveries, want 10", n)
	}
	for seq := range a.seen {
		if b.seen[seq] > 0 {
			t.Fatalf("stream sequence %s went to both instances", seq)
		}
	}
}

func TestConsume_TwoGroupsEachReceiveEveryMessage(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	var a, b countingSubscriber
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit"}, Handler: a.handle}})
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "billing"}, Handler: b.handle}})

	publishEvents(t, url, 5)
	waitFor(t, func() bool { return a.total() >= 5 && b.total() >= 5 })
	time.Sleep(100 * time.Millisecond) // give an unwanted extra delivery time to show up
	if a.total() != 5 || b.total() != 5 {
		t.Fatalf("groups handled %d and %d deliveries, want 5 each", a.total(), b.total())
	}
}

func TestConsume_EveryNoneInstanceReceivesEveryMessage(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: mesh.ConsumerGroupNone}
	var a, b countingSubscriber
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: a.handle}})
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: b.handle}})

	publishEvents(t, url, 5)
	waitFor(t, func() bool { return a.total() >= 5 && b.total() >= 5 })
	time.Sleep(100 * time.Millisecond) // give an unwanted extra delivery time to show up
	if a.total() != 5 || b.total() != 5 {
		t.Fatalf("instances handled %d and %d deliveries, want 5 each", a.total(), b.total())
	}
}

// ackPending is how many delivered messages the consumer is waiting on.
func ackPending(t *testing.T, stream jetstream.Stream, name string) int {
	t.Helper()
	cons, err := stream.Consumer(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return cons.CachedInfo().NumAckPending
}

func TestConsume_AckPolicyDouble(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	var s countingSubscriber
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", AckPolicyKey: "double", AckWaitKey: "1s"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: s.handle}})

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return s.total() == 1 && ackPending(t, stream, "audit") == 0 })
	time.Sleep(1500 * time.Millisecond) // past ack_wait, so an unacked message would come again
	if s.total() != 1 {
		t.Fatalf("handled %d deliveries, want 1", s.total())
	}
}

func TestConsume_AckPolicyExplicit(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	var s countingSubscriber
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", AckPolicyKey: "explicit", AckWaitKey: "1s"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: s.handle}})

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return s.total() == 1 && ackPending(t, stream, "audit") == 0 })
	time.Sleep(1500 * time.Millisecond) // past ack_wait, so an unacked message would come again
	if s.total() != 1 {
		t.Fatalf("handled %d deliveries, want 1", s.total())
	}
}

func TestConsume_AckPolicyNone(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	var calls atomic.Int32
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", AckPolicyKey: "none"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		calls.Add(1)
		return errors.New("boom")
	}}}, WithLogger(quietLogger()))

	cons, err := stream.Consumer(context.Background(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	if p := cons.CachedInfo().Config.AckPolicy; p != jetstream.AckNonePolicy {
		t.Fatalf("consumer ack policy %s, want none", p)
	}
	publishEvents(t, url, 1)
	waitFor(t, func() bool { return calls.Load() == 1 })
	time.Sleep(300 * time.Millisecond) // a nacked message would come again right away
	if n := calls.Load(); n != 1 {
		t.Fatalf("failing handler called %d times, want 1", n)
	}
}

func TestConsume_OrderedConsumerReceivesEveryMessage(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	var s countingSubscriber
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: mesh.ConsumerGroupNone, AckPolicyKey: "none"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: s.handle}})

	publishEvents(t, url, 5)
	waitFor(t, func() bool { return s.total() >= 5 })
	if s.total() != 5 {
		t.Fatalf("handled %d deliveries, want 5", s.total())
	}
}

func TestConsume_FailingHandlerRedeliveredUntilMaxDeliver(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	var calls atomic.Int32
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", MaxDeliverKey: "3"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		calls.Add(1)
		return errors.New("boom")
	}}}, WithLogger(quietLogger()))

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return calls.Load() >= 3 })
	time.Sleep(300 * time.Millisecond) // give an unwanted extra delivery time to show up
	if n := calls.Load(); n != 3 {
		t.Fatalf("handler called %d times, want 3", n)
	}
}

func TestConsume_PanickingHandlerRedelivered(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	var calls atomic.Int32
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		if calls.Add(1) == 1 {
			panic("kaboom")
		}
		return nil
	}}}, WithLogger(quietLogger()))

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return calls.Load() >= 2 })
}

func TestConsume_NakDelayDelaysRedelivery(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	var mu sync.Mutex
	var times []time.Time
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", NakDelayKey: "500ms"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		mu.Lock()
		defer mu.Unlock()
		times = append(times, time.Now())
		if len(times) == 1 {
			return errors.New("boom")
		}
		return nil
	}}}, WithLogger(quietLogger()))

	publishEvents(t, url, 1)
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(times) >= 2
	})
	mu.Lock()
	defer mu.Unlock()
	if gap := times[1].Sub(times[0]); gap < 450*time.Millisecond {
		t.Fatalf("redelivered after %s, want at least 500ms", gap)
	}
}

func TestConsume_DeliveryMetadata(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	type seenDelivery struct {
		md       map[string]string
		deadline time.Time
		ok       bool
	}
	seen := make(chan seenDelivery, 1)
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit", AckWaitKey: "10s"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		d, ok := ctx.Deadline()
		seen <- seenDelivery{md: m.Metadata, deadline: d, ok: ok}
		return nil
	}}})

	published := time.Now()
	publishEvents(t, url, 1)
	var got seenDelivery
	select {
	case got = <-seen:
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}

	want := map[string]string{
		mesh.DeliveryAttemptKey:     "1",
		StreamMetadataKey:           "EVENTS",
		ConsumerMetadataKey:         "audit",
		StreamSequenceMetadataKey:   "1",
		ConsumerSequenceMetadataKey: "1",
		NumPendingMetadataKey:       "0",
	}
	for k, v := range want {
		if got.md[k] != v {
			t.Errorf("%s = %q, want %q", k, got.md[k], v)
		}
	}
	stored, err := time.Parse(time.RFC3339Nano, got.md[TimestampMetadataKey])
	if err != nil || stored.Before(published.Add(-time.Second)) || stored.After(time.Now()) {
		t.Errorf("%s = %q, want the time of the publish", TimestampMetadataKey, got.md[TimestampMetadataKey])
	}
	deadline, err := time.Parse(time.RFC3339Nano, got.md[mesh.DeadlineKey])
	if err != nil || deadline.Before(published.Add(10*time.Second)) || deadline.After(time.Now().Add(10*time.Second)) {
		t.Errorf("%s = %q, want 10s after delivery", mesh.DeadlineKey, got.md[mesh.DeadlineKey])
	}
	if !got.ok || got.deadline.Format(time.RFC3339Nano) != got.md[mesh.DeadlineKey] {
		t.Errorf("context deadline %s, want %s", got.deadline.Format(time.RFC3339Nano), got.md[mesh.DeadlineKey])
	}
}

func TestStart_StreamCoveringEndpointSubject(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	createStream(t, url, "REQUESTS", "test.echo")
	r := newRuntime(t, testConfig(url),
		[]mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}},
		[]mesh.Subscriber{{Target: eventTarget, Metadata: map[string]string{StreamKey: "EVENTS"}, Handler: okSubscriber}},
	)

	if err := r.Start(context.Background()); !errors.Is(err, ErrStreamCoversRoute) {
		t.Fatalf("want ErrStreamCoversRoute, got %v", err)
	}
}

func TestStart_NoJetStreamSubscriberSkipsStreamCheck(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "REQUESTS", "test.echo")
	r := newRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, []mesh.Subscriber{{Target: eventTarget, Handler: okSubscriber}})

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_ = r.Stop(context.Background())
}

func TestStop_NaksMessagesPastTheDrain(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS", mesh.ConsumerGroupKey: "audit"}
	logs := &logRecords{}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "1"
	r := startRuntime(t, cfg, nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		entered <- struct{}{}
		<-release
		return nil
	}}}, WithLogger(slog.New(logs)))

	c := testClient(t, testConfig(url))
	for _, id := range []string{"first", "second"} {
		msg := mesh.Message{Target: eventTarget, Metadata: map[string]string{mesh.MessageIDKey: id}}
		if err := c.Publish(context.Background(), msg, map[string]string{StreamKey: "EVENTS"}); err != nil {
			t.Fatal(err)
		}
	}
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := r.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop: want context.DeadlineExceeded, got %v", err)
	}

	warnings := logs.withMessage("nats: nacked a message at the end of the drain")
	if len(warnings) != 2 {
		t.Fatalf("logged %d warnings, want 2", len(warnings))
	}
	byID := map[string]map[string]slog.Value{}
	for _, w := range warnings {
		byID[w["message_id"].String()] = w
	}
	for id, running := range map[string]bool{"first": true, "second": false} {
		w := byID[id]
		if w == nil {
			t.Fatalf("no warning for message %s", id)
		}
		if w["subject"].String() != "test.event" || w["stream"].String() != "EVENTS" || w["consumer"].String() != "audit" {
			t.Errorf("message %s: warning %v", id, w)
		}
		if w["stream_sequence"].Uint64() == 0 {
			t.Errorf("message %s: no stream_sequence", id)
		}
		if w["handler_running"].Bool() != running {
			t.Errorf("message %s: handler_running %v, want %v", id, w["handler_running"].Bool(), running)
		}
	}

	var s countingSubscriber
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: s.handle}})
	waitFor(t, func() bool { return s.total() >= 2 })
}
