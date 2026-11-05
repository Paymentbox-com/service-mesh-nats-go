package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
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
	md := map[string]string{StreamKey: "EVENTS", AckWaitKey: "10s"}

	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: okSubscriber}})
	cons, err := stream.Consumer(context.Background(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	if got := cons.CachedInfo().Config; got.AckWait != 10*time.Second || got.InactiveThreshold != 7*24*time.Hour || got.MaxDeliver != 5 {
		t.Fatalf("created consumer settings %+v", got)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	md[AckWaitKey] = "20s"
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: okSubscriber}})
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
	md := map[string]string{StreamKey: "EVENTS", CreateConsumerKey: "false", AckWaitKey: "10s"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
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
	md := map[string]string{StreamKey: "EVENTS", CreateConsumerKey: "false"}
	r := newRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: okSubscriber}})

	if err := r.Start(context.Background()); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("want jetstream.ErrConsumerNotFound, got %v", err)
	}
}

func TestStart_NoneSubscribersGetTheirOwnConsumers(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS"}
	for i := 0; i < 2; i++ {
		startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: mesh.ConsumerGroupNone, Metadata: md, Handler: okSubscriber}})
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
	md := map[string]string{StreamKey: "EVENTS"}
	var a, b countingSubscriber
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: a.handle}})
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: b.handle}})

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
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: map[string]string{StreamKey: "EVENTS"}, Handler: a.handle}})
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "billing", Metadata: map[string]string{StreamKey: "EVENTS"}, Handler: b.handle}})

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
	md := map[string]string{StreamKey: "EVENTS"}
	var a, b countingSubscriber
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: mesh.ConsumerGroupNone, Metadata: md, Handler: a.handle}})
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: mesh.ConsumerGroupNone, Metadata: md, Handler: b.handle}})

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
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: "double", AckWaitKey: "500ms"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: s.handle}})

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return s.total() == 1 && ackPending(t, stream, "audit") == 0 })
	time.Sleep(800 * time.Millisecond) // past ack_wait, so an unacked message would come again
	if s.total() != 1 {
		t.Fatalf("handled %d deliveries, want 1", s.total())
	}
}

func TestConsume_AckPolicyExplicit(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	var s countingSubscriber
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: "explicit", AckWaitKey: "500ms"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: s.handle}})

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return s.total() == 1 && ackPending(t, stream, "audit") == 0 })
	time.Sleep(800 * time.Millisecond) // past ack_wait, so an unacked message would come again
	if s.total() != 1 {
		t.Fatalf("handled %d deliveries, want 1", s.total())
	}
}

func TestConsume_AckPolicyNone(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	var calls atomic.Int32
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: "none"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
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
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: "none"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: mesh.ConsumerGroupNone, Metadata: md, Handler: s.handle}})

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
	md := map[string]string{StreamKey: "EVENTS", MaxDeliverKey: "3"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
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
	md := map[string]string{StreamKey: "EVENTS"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		if calls.Add(1) == 1 {
			panic("kaboom")
		}
		return nil
	}}}, WithLogger(quietLogger()))

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return calls.Load() >= 2 })
}

func TestConsume_FailureOnLastDeliveryLogged(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	md := map[string]string{StreamKey: "EVENTS", MaxDeliverKey: "2"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		return errors.New("boom")
	}}}, WithLogger(slog.New(logs)))

	c := testClient(t, testConfig(url))
	msg := mesh.Message{Target: eventTarget, Metadata: map[string]string{mesh.MessageIDKey: "id-1"}}
	if err := c.Publish(context.Background(), msg, map[string]string{StreamKey: "EVENTS"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(logs.withMessage("nats: message will not be delivered again")) > 0 })
	time.Sleep(300 * time.Millisecond) // give an unwanted extra record time to show up
	recs := logs.withMessage("nats: message will not be delivered again")
	if len(recs) != 1 {
		t.Fatalf("%d final failure records, want 1", len(recs))
	}
	rec := recs[0]
	if rec["subject"].String() != "test.event" ||
		rec["stream"].String() != "EVENTS" ||
		rec["consumer"].String() != "audit" ||
		rec["stream_sequence"].Uint64() != 1 ||
		rec["delivery_attempt"].Uint64() != 2 ||
		rec["max_deliver"].Int64() != 2 ||
		rec["message_id"].String() != "id-1" {
		t.Fatalf("log record %v", rec)
	}
	if level := logs.levels("nats: message will not be delivered again")[0]; level != slog.LevelError {
		t.Fatalf("logged at %v, want ERROR", level)
	}
}

func TestConsume_FailureBeforeLastDeliveryNotLoggedAsFinal(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	var calls atomic.Int32
	md := map[string]string{StreamKey: "EVENTS", MaxDeliverKey: "2"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		if calls.Add(1) == 1 {
			return errors.New("boom")
		}
		return nil
	}}}, WithLogger(slog.New(logs)))

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return calls.Load() == 2 })
	time.Sleep(300 * time.Millisecond) // give an unwanted record time to show up
	if recs := logs.withMessage("nats: message will not be delivered again"); len(recs) != 0 {
		t.Fatalf("failure on delivery 1 of 2 logged as final: %v", recs)
	}
}

func TestConsume_UnlimitedMaxDeliverNeverLoggedAsFinal(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	if _, err := stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       "audit",
		FilterSubject: "test.event",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    -1,
	}); err != nil {
		t.Fatal(err)
	}
	logs := &logRecords{}
	var calls atomic.Int32
	md := map[string]string{StreamKey: "EVENTS", CreateConsumerKey: "false"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		calls.Add(1)
		return errors.New("boom")
	}}}, WithLogger(slog.New(logs)))

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return calls.Load() >= 3 })
	if recs := logs.withMessage("nats: message will not be delivered again"); len(recs) != 0 {
		t.Fatalf("failure with unlimited max_deliver logged as final: %v", recs)
	}
}

func TestConsume_HandlerFailureLogsDeliveryAttempt(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	md := map[string]string{StreamKey: "EVENTS", MaxDeliverKey: "2"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		return errors.New("boom")
	}}}, WithLogger(slog.New(logs)))

	publishEvents(t, url, 1)
	waitFor(t, func() bool { return len(logs.withMessage("nats: subscriber handler failed")) == 2 })
	recs := logs.withMessage("nats: subscriber handler failed")
	if recs[0]["delivery_attempt"].Uint64() != 1 || recs[1]["delivery_attempt"].Uint64() != 2 {
		t.Fatalf("delivery attempts %v and %v, want 1 and 2", recs[0]["delivery_attempt"], recs[1]["delivery_attempt"])
	}
}

func TestConsume_LateStartLogged(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	holdTarget := mesh.Target{Segments: []string{"test", "hold"}, Kind: mesh.KindTopic}
	entered := make(chan struct{}, 1)
	hold := mesh.Subscriber{Target: holdTarget, Handler: func(ctx context.Context, m mesh.Message) error {
		entered <- struct{}{}
		time.Sleep(500 * time.Millisecond)
		return nil
	}}
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "1"
	md := map[string]string{StreamKey: "EVENTS", AckWaitKey: "200ms", MaxDeliverKey: "1"}
	startRuntime(t, cfg, nil, []mesh.Subscriber{hold, {Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: okSubscriber}}, WithLogger(slog.New(logs)))

	c := testClient(t, testConfig(url))
	if err := c.Publish(context.Background(), mesh.Message{Target: holdTarget}, nil); err != nil {
		t.Fatal(err)
	}
	<-entered
	msg := mesh.Message{Target: eventTarget, Metadata: map[string]string{mesh.MessageIDKey: "id-1"}}
	if err := c.Publish(context.Background(), msg, map[string]string{StreamKey: "EVENTS"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(logs.withMessage("nats: handler started after its ack deadline")) > 0 })
	rec := logs.withMessage("nats: handler started after its ack deadline")[0]
	if rec["subject"].String() != "test.event" ||
		rec["stream"].String() != "EVENTS" ||
		rec["consumer"].String() != "audit" ||
		rec["stream_sequence"].Uint64() != 1 ||
		rec["delivery_attempt"].Uint64() != 1 ||
		rec["message_id"].String() != "id-1" ||
		rec["late_by"].Duration() <= 0 ||
		rec["skipped"].Int64() != 0 {
		t.Fatalf("log record %v", rec)
	}
	if level := logs.levels("nats: handler started after its ack deadline")[0]; level != slog.LevelWarn {
		t.Fatalf("logged at %v, want WARN", level)
	}
}

// The late delivery is the second attempt of the consumer's second message,
// on a consumer that skipped the stream's first two messages, so its stream
// sequence, consumer sequence, and delivery attempt all differ.
func TestConsume_LateRedeliveryLogged(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	publishEvents(t, url, 2)
	logs := &logRecords{}
	var bCalls atomic.Int32
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "1"
	md := map[string]string{StreamKey: "EVENTS", DeliverPolicyKey: "new", AckWaitKey: "200ms", MaxDeliverKey: "2"}
	startRuntime(t, cfg, nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		if string(m.Payload) != "b" {
			return nil
		}
		if bCalls.Add(1) == 1 {
			// Holds the only handler slot past the ack wait, so the server
			// sends b again and that delivery waits past its deadline.
			time.Sleep(500 * time.Millisecond)
			return errors.New("boom")
		}
		return nil
	}}}, WithLogger(slog.New(logs)))

	c := testClient(t, testConfig(url))
	for _, payload := range []string{"a", "b"} {
		if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget, Payload: []byte(payload)}, map[string]string{StreamKey: "EVENTS"}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return len(logs.withMessage("nats: handler started after its ack deadline")) > 0 })
	time.Sleep(300 * time.Millisecond) // give an unwanted extra record time to show up
	recs := logs.withMessage("nats: handler started after its ack deadline")
	if len(recs) != 1 {
		t.Fatalf("%d late start records, want 1", len(recs))
	}
	rec := recs[0]
	if rec["stream_sequence"].Uint64() != 4 || rec["delivery_attempt"].Uint64() != 2 {
		t.Fatalf("log record %v, want stream_sequence 4 and delivery_attempt 2", rec)
	}
	if id, ok := rec["message_id"]; ok {
		t.Fatalf("message_id %q on a message without one", id)
	}
}

func TestConsume_StartBeforeDeadlineNotLogged(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	got := make(chan struct{}, 1)
	md := map[string]string{StreamKey: "EVENTS"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		got <- struct{}{}
		return nil
	}}}, WithLogger(slog.New(logs)))

	publishEvents(t, url, 1)
	select {
	case <-got:
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
	if recs := logs.withMessage("nats: handler started after its ack deadline"); len(recs) != 0 {
		t.Fatalf("start before the deadline logged as late: %v", recs)
	}
}

func TestConsume_LateStartsWithinIntervalSkipped(t *testing.T) {
	old := lateStartLogInterval
	lateStartLogInterval = time.Second
	t.Cleanup(func() { lateStartLogInterval = old })

	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	var handled atomic.Int32
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "1"
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: "explicit", AckWaitKey: "100ms", MaxDeliverKey: "1"}
	startRuntime(t, cfg, nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		time.Sleep(300 * time.Millisecond)
		handled.Add(1)
		return nil
	}}}, WithLogger(slog.New(logs)))

	// Each message after the first waits 300ms for the handler slot, past
	// its 100ms deadline, so messages 2 and 3 start late within the interval.
	publishEvents(t, url, 3)
	waitFor(t, func() bool { return handled.Load() == 3 })
	if n := len(logs.withMessage("nats: handler started after its ack deadline")); n != 1 {
		t.Fatalf("%d late-start records for two late starts within the interval, want 1", n)
	}

	// Message 5 starts late after the interval, and is logged with the one
	// skipped.
	time.Sleep(time.Second)
	publishEvents(t, url, 2)
	waitFor(t, func() bool { return len(logs.withMessage("nats: handler started after its ack deadline")) == 2 })
	rec := logs.withMessage("nats: handler started after its ack deadline")[1]
	if rec["stream_sequence"].Uint64() != 5 || rec["skipped"].Int64() != 1 {
		t.Fatalf("log record %v, want stream_sequence 5 and skipped 1", rec)
	}

	// Message 7 starts late after another interval with none skipped.
	time.Sleep(time.Second)
	publishEvents(t, url, 2)
	waitFor(t, func() bool { return len(logs.withMessage("nats: handler started after its ack deadline")) == 3 })
	rec = logs.withMessage("nats: handler started after its ack deadline")[2]
	if rec["stream_sequence"].Uint64() != 7 || rec["skipped"].Int64() != 0 {
		t.Fatalf("log record %v, want stream_sequence 7 and skipped 0", rec)
	}
}

func TestConsume_LateStartWithoutAcksNotLogged(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	holdTarget := mesh.Target{Segments: []string{"test", "hold"}, Kind: mesh.KindTopic}
	entered := make(chan struct{}, 1)
	hold := mesh.Subscriber{Target: holdTarget, Handler: func(ctx context.Context, m mesh.Message) error {
		entered <- struct{}{}
		time.Sleep(300 * time.Millisecond)
		return nil
	}}
	got := make(chan struct{}, 1)
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "1"
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: "none"}
	startRuntime(t, cfg, nil, []mesh.Subscriber{hold, {Target: eventTarget, ConsumerGroup: mesh.ConsumerGroupNone, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		got <- struct{}{}
		return nil
	}}}, WithLogger(slog.New(logs)))

	c := testClient(t, testConfig(url))
	if err := c.Publish(context.Background(), mesh.Message{Target: holdTarget}, nil); err != nil {
		t.Fatal(err)
	}
	<-entered
	publishEvents(t, url, 1)
	select {
	case <-got:
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
	if recs := logs.withMessage("nats: handler started after its ack deadline"); len(recs) != 0 {
		t.Fatalf("consumer without acks logged a late start: %v", recs)
	}
}

func TestConsume_DeletedConsumerLogged(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	md := map[string]string{StreamKey: "EVENTS"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: okSubscriber}}, WithLogger(slog.New(logs)))

	if err := stream.DeleteConsumer(context.Background(), "audit"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(logs.withMessage("nats: consumer missing")) > 0 })
	rec := logs.withMessage("nats: consumer missing")[0]
	if rec["subject"].String() != "test.event" ||
		rec["stream"].String() != "EVENTS" ||
		rec["consumer"].String() != "audit" ||
		!errors.Is(rec["error"].Any().(error), jetstream.ErrConsumerDeleted) {
		t.Fatalf("log record %v", rec)
	}
	if level := logs.levels("nats: consumer missing")[0]; level != slog.LevelError {
		t.Fatalf("logged at %v, want ERROR", level)
	}
}

// A consumer deleted while the client is disconnected gets no 409 from the
// server. After the reconnect, its pulls get no responders.
func TestConsume_ConsumerDeletedWhileDisconnectedLogged(t *testing.T) {
	ns := startServerHandle(t)
	stream := createStream(t, ns.ClientURL(), "EVENTS", "test.event")
	logs := &logRecords{}
	startRuntimeAudit(t, ns, stream, slog.New(logs), okSubscriber, true)
	waitFor(t, func() bool { return len(logs.withMessage("nats: consumer missing")) > 0 })
	rec := logs.withMessage("nats: consumer missing")[0]
	if rec["consumer"].String() != "audit" || !errors.Is(rec["error"].Any().(error), natsio.ErrNoResponders) {
		t.Fatalf("log record %v", rec)
	}
}

// startServerHandle is startServer for a test that needs the server itself.
func startServerHandle(t *testing.T) *server.Server {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(testWait) {
		t.Fatal("nats-server did not become ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns
}

// startRuntimeAudit starts a runtime with one subscriber in the group
// "audit" on stream EVENTS, pulling with a 1s heartbeat, and then deletes
// the group's consumer. With disconnect, the server drops the runtime's
// connection before the delete, so the runtime sees no 409.
func startRuntimeAudit(t *testing.T, ns *server.Server, stream jetstream.Stream, logger *slog.Logger, h mesh.SubscriberHandler, disconnect bool) {
	t.Helper()
	cfg := testConfig(ns.ClientURL())
	cfg[PullExpiryKey] = "2s"
	cfg[PullHeartbeatKey] = "1s"
	md := map[string]string{StreamKey: "EVENTS"}
	r := startRuntime(t, cfg, nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: h}}, WithLogger(logger))
	if disconnect {
		id, err := r.client.nc.GetClientID()
		if err != nil {
			t.Fatal(err)
		}
		if err := ns.DisconnectClientByID(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.DeleteConsumer(context.Background(), "audit"); err != nil {
		t.Fatal(err)
	}
}

// shortMissingLogInterval sets missingLogInterval to 100ms for one test.
func shortMissingLogInterval(t *testing.T) {
	old := missingLogInterval
	missingLogInterval = 100 * time.Millisecond
	t.Cleanup(func() { missingLogInterval = old })
}

// recreateAudit creates the group "audit"'s consumer again, as another
// instance's Start would.
func recreateAudit(t *testing.T, stream jetstream.Stream) {
	t.Helper()
	if _, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{Durable: "audit", FilterSubject: "test.event", AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
		t.Fatal(err)
	}
}

// missingCountSettles waits three intervals and returns how many
// "nats: consumer missing" records there are.
func missingCountSettles(logs *logRecords) int {
	time.Sleep(3 * missingLogInterval)
	return len(logs.withMessage("nats: consumer missing"))
}

func TestConsume_MissingConsumerLoggedAgain(t *testing.T) {
	shortMissingLogInterval(t)
	ns := startServerHandle(t)
	stream := createStream(t, ns.ClientURL(), "EVENTS", "test.event")
	logs := &logRecords{}
	startRuntimeAudit(t, ns, stream, slog.New(logs), okSubscriber, false)

	waitFor(t, func() bool { return len(logs.withMessage("nats: consumer missing")) >= 3 })
	rec := logs.withMessage("nats: consumer missing")[2]
	if rec["consumer"].String() != "audit" {
		t.Fatalf("log record %v", rec)
	}
	if _, ok := rec["error"]; ok {
		t.Fatalf("repeated record has an error: %v", rec)
	}
}

// nats.go stopped consuming on the 409, so a consumer created again still
// gets nothing from this runtime.
func TestConsume_StoppedConsumerLoggedAfterRecreated(t *testing.T) {
	shortMissingLogInterval(t)
	ns := startServerHandle(t)
	stream := createStream(t, ns.ClientURL(), "EVENTS", "test.event")
	logs := &logRecords{}
	startRuntimeAudit(t, ns, stream, slog.New(logs), okSubscriber, false)
	waitFor(t, func() bool { return len(logs.withMessage("nats: consumer missing")) > 0 })

	recreateAudit(t, stream)
	n := missingCountSettles(logs)
	if m := missingCountSettles(logs); m <= n {
		t.Fatalf("%d records, then %d, want more", n, m)
	}
}

// nats.go kept pulling after the disconnect, so a consumer created again
// delivers to this runtime, and is no longer logged.
func TestConsume_PulledConsumerNotLoggedAfterRecreated(t *testing.T) {
	shortMissingLogInterval(t)
	ns := startServerHandle(t)
	stream := createStream(t, ns.ClientURL(), "EVENTS", "test.event")
	logs := &logRecords{}
	var got atomic.Int32
	startRuntimeAudit(t, ns, stream, slog.New(logs), func(ctx context.Context, m mesh.Message) error {
		got.Add(1)
		return nil
	}, true)
	waitFor(t, func() bool { return len(logs.withMessage("nats: consumer missing")) > 0 })

	recreateAudit(t, stream)
	publishEvents(t, ns.ClientURL(), 1)
	waitFor(t, func() bool { return got.Load() == 1 })
	n := missingCountSettles(logs)
	if m := missingCountSettles(logs); m != n {
		t.Fatalf("%d records, then %d, want no more", n, m)
	}
}

func TestConsume_DeletedOrderedConsumerNotLoggedAsMissing(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	var got atomic.Int32
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: "none"}
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: mesh.ConsumerGroupNone, Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		got.Add(1)
		return nil
	}}}, WithLogger(slog.New(logs)))

	rc, _ := r.Receiver(eventTarget)
	if err := stream.DeleteConsumer(context.Background(), rc.Consumer().CachedInfo().Name); err != nil {
		t.Fatal(err)
	}
	// The message arrives only after nats.go has reported the deleted
	// consumer and created it again.
	publishEvents(t, url, 1)
	waitFor(t, func() bool { return got.Load() == 1 })
	if recs := logs.withMessage("nats: consumer missing"); len(recs) != 0 {
		t.Fatalf("ordered consumer logged as missing: %v", recs)
	}
}

func TestConsume_NakDelayDelaysRedelivery(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	var mu sync.Mutex
	var times []time.Time
	md := map[string]string{StreamKey: "EVENTS", NakDelayKey: "250ms"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
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
	if gap := times[1].Sub(times[0]); gap < 200*time.Millisecond {
		t.Fatalf("redelivered after %s, want at least 250ms", gap)
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
	md := map[string]string{StreamKey: "EVENTS", AckWaitKey: "10s"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
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

func TestStart_StreamCoveringEndpointSubjectWithoutJetStreamSubscriber(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "REQUESTS", "test.echo")
	r := newRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, []mesh.Subscriber{{Target: eventTarget, Handler: okSubscriber}})

	if err := r.Start(context.Background()); !errors.Is(err, ErrStreamCoversRoute) {
		t.Fatalf("want ErrStreamCoversRoute, got %v", err)
	}
}

func TestStart_ServerWithoutJetStream(t *testing.T) {
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(testWait) {
		t.Fatal("nats-server did not become ready")
	}
	t.Cleanup(ns.Shutdown)
	r := newRuntime(t, testConfig(ns.ClientURL()), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_ = r.Stop(context.Background())
}

func TestStop_NaksMessagesPastTheDrain(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{StreamKey: "EVENTS"}
	logs := &logRecords{}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "1"
	r := startRuntime(t, cfg, nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
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
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: s.handle}})
	waitFor(t, func() bool { return s.total() >= 2 })
}

func TestPublish_RequestTimeoutBoundsJetStreamPublish(t *testing.T) {
	// A core subscriber and no stream: the publish waits for an ack that never
	// comes, until request_timeout.
	url := startServer(t)
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if _, err := nc.SubscribeSync("test.event"); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	c := testClient(t, testConfig(url))

	start := time.Now()
	err = c.Publish(context.Background(), mesh.Message{Target: eventTarget}, map[string]string{StreamKey: "NOPE", RequestTimeoutKey: "200ms"})
	if !errors.Is(err, natsio.ErrTimeout) || time.Since(start) > time.Second {
		t.Fatalf("want natsio.ErrTimeout after about 200ms, got %v after %s", err, time.Since(start))
	}
}

func TestStart_OrderedConsumerInactiveThreshold(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:        eventTarget,
		ConsumerGroup: mesh.ConsumerGroupNone,
		Metadata:      map[string]string{StreamKey: "EVENTS", AckPolicyKey: "none"},
		Handler:       okSubscriber,
	}})

	rc, ok := r.Receiver(eventTarget)
	if !ok {
		t.Fatal("no receiver for the target")
	}
	info, err := rc.Consumer().Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Config.InactiveThreshold; got != 5*time.Minute {
		t.Fatalf("ordered consumer inactive threshold %s, want 5m", got)
	}
}

func TestStart_NoneConsumerWithAcksInactiveThreshold(t *testing.T) {
	// A none consumer that acks is named anew at every Start, so the
	// server's 5 second default removes it soon after its instance stops.
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:        eventTarget,
		ConsumerGroup: mesh.ConsumerGroupNone,
		Metadata:      map[string]string{StreamKey: "EVENTS"},
		Handler:       okSubscriber,
	}})

	rc, ok := r.Receiver(eventTarget)
	if !ok {
		t.Fatal("no receiver for the target")
	}
	info, err := rc.Consumer().Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Config.InactiveThreshold; got != 5*time.Second {
		t.Fatalf("none consumer inactive threshold %s, want the server's 5s", got)
	}
}

func TestConsume_OrderedConsumerDeliverPolicyNew(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	c := testClient(t, testConfig(url))
	publish := func(payload string) {
		t.Helper()
		if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget, Payload: []byte(payload)}, map[string]string{StreamKey: "EVENTS"}); err != nil {
			t.Fatal(err)
		}
	}
	publish("before")

	got := make(chan string, 2)
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:        eventTarget,
		ConsumerGroup: mesh.ConsumerGroupNone,
		Metadata:      map[string]string{StreamKey: "EVENTS", AckPolicyKey: "none", DeliverPolicyKey: "new"},
		Handler: func(ctx context.Context, m mesh.Message) error {
			got <- string(m.Payload)
			return nil
		},
	}})
	publish("after")

	select {
	case p := <-got:
		if p != "after" {
			t.Fatalf("ordered consumer with deliver_policy new delivered %q first, want only \"after\"", p)
		}
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message published after Start")
	}
	select {
	case p := <-got:
		t.Fatalf("ordered consumer delivered a second message %q, want only \"after\"", p)
	case <-time.After(100 * time.Millisecond): // give an unwanted extra delivery time to show up
	}
}

func TestConsume_DeadlineUsesBackoffEntry(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	seen := make(chan map[string]string, 1)
	md := map[string]string{StreamKey: "EVENTS", BackoffKey: "1s,5s", MaxDeliverKey: "3"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		seen <- m.Metadata
		return nil
	}}})

	published := time.Now()
	publishEvents(t, url, 1)
	var got map[string]string
	select {
	case got = <-seen:
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
	deadline, err := time.Parse(time.RFC3339Nano, got[mesh.DeadlineKey])
	if err != nil {
		t.Fatalf("%s = %q: %v", mesh.DeadlineKey, got[mesh.DeadlineKey], err)
	}
	if until := deadline.Sub(published); until < 900*time.Millisecond || until > 2*time.Second {
		t.Fatalf("first delivery's deadline is %s after the publish, want about 1s from backoff[0]", until)
	}
}

func TestConsume_OutsideConsumerWithoutAcks(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	if _, err := stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       "audit",
		FilterSubject: "test.event",
		AckPolicy:     jetstream.AckNonePolicy,
	}); err != nil {
		t.Fatal(err)
	}
	type seenDelivery struct {
		md map[string]string
		ok bool
	}
	seen := make(chan seenDelivery, 1)
	md := map[string]string{StreamKey: "EVENTS", CreateConsumerKey: "false"}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
		_, ok := ctx.Deadline()
		seen <- seenDelivery{md: m.Metadata, ok: ok}
		return nil
	}}})

	publishEvents(t, url, 1)
	select {
	case got := <-seen:
		if v, has := got.md[mesh.DeadlineKey]; has || got.ok {
			t.Fatalf("handler of a consumer that does not ack got %s %q and a context deadline %v", mesh.DeadlineKey, v, got.ok)
		}
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
}

// ackReply returns whether the runtime's ack for a delivery carried a reply
// subject, which DoubleAck sets and Ack does not.
func ackReply(t *testing.T, url, ackPolicy string) bool {
	t.Helper()
	createStream(t, url, "EVENTS", "test.event")
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	acks, err := nc.SubscribeSync("$JS.ACK.EVENTS.>")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	md := map[string]string{StreamKey: "EVENTS", AckPolicyKey: ackPolicy}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: okSubscriber}})

	publishEvents(t, url, 1)
	m, err := acks.NextMsg(testWait)
	if err != nil {
		t.Fatalf("no ack seen: %v", err)
	}
	return m.Reply != ""
}

func TestConsume_AckPolicyDoubleWaitsForConfirmation(t *testing.T) {
	if !ackReply(t, startServer(t), "double") {
		t.Fatal("ack_policy double sent its ack without a reply subject, so it did not wait for the server")
	}
}

func TestConsume_AckPolicyExplicitDoesNotWait(t *testing.T) {
	if ackReply(t, startServer(t), "explicit") {
		t.Fatal("ack_policy explicit sent its ack with a reply subject, so it waited for the server")
	}
}

func TestStop_NaksMessagesBufferedWhenTheDrainEnds(t *testing.T) {
	// With one handler slot, one message is running and one waits for the
	// slot when the drain ends. The other three are still in nats.go's buffer
	// and reach the runtime only after the drain has ended.
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	logs := &logRecords{}
	entered := make(chan struct{}, 5)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "1"
	r := startRuntime(t, cfg, nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: map[string]string{StreamKey: "EVENTS"}, Handler: func(ctx context.Context, m mesh.Message) error {
		entered <- struct{}{}
		<-release
		return nil
	}}}, WithLogger(slog.New(logs)))

	c := testClient(t, testConfig(url))
	for i := 1; i <= 5; i++ {
		msg := mesh.Message{Target: eventTarget, Metadata: map[string]string{mesh.MessageIDKey: fmt.Sprintf("m%d", i)}}
		if err := c.Publish(context.Background(), msg, map[string]string{StreamKey: "EVENTS"}); err != nil {
			t.Fatal(err)
		}
	}
	<-entered
	time.Sleep(200 * time.Millisecond) // let the other four reach the client

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := r.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop: want context.DeadlineExceeded, got %v", err)
	}

	warnings := logs.withMessage("nats: nacked a message at the end of the drain")
	ids := map[string]bool{}
	for _, w := range warnings {
		ids[w["message_id"].String()] = true
	}
	if len(warnings) != 5 || len(ids) != 5 {
		t.Fatalf("logged %d warnings for %d messages, want one for each of 5", len(warnings), len(ids))
	}
}

func TestStart_FailureUnsubscribesEarlierEndpoints(t *testing.T) {
	url := startServer(t)
	r := newRuntime(t, testConfig(url),
		[]mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}},
		[]mesh.Subscriber{{Target: eventTarget, Metadata: map[string]string{StreamKey: "MISSING"}, Handler: okSubscriber}},
	)
	if err := r.Start(context.Background()); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("Start: want jetstream.ErrStreamNotFound, got %v", err)
	}

	c := testClient(t, testConfig(url))
	if _, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil); !errors.Is(err, ErrNoResponders) {
		t.Fatalf("request after a failed Start: want nats.ErrNoResponders, got %v", err)
	}
}

func TestStart_FailureStopsEarlierConsumers(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	other := mesh.Target{Segments: []string{"test", "other"}, Kind: mesh.KindTopic}
	r := newRuntime(t, testConfig(url), nil, []mesh.Subscriber{
		{Target: eventTarget, ConsumerGroup: "audit", Metadata: map[string]string{StreamKey: "EVENTS"}, Handler: okSubscriber},
		{Target: other, Metadata: map[string]string{StreamKey: "MISSING"}, Handler: okSubscriber},
	})
	if err := r.Start(context.Background()); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("Start: want jetstream.ErrStreamNotFound, got %v", err)
	}

	publishEvents(t, url, 1)
	time.Sleep(300 * time.Millisecond) // give a pull that should not happen time to take the message
	cons, err := stream.Consumer(context.Background(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	info, err := cons.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.Delivered.Consumer != 0 {
		t.Fatalf("consumer delivered %d messages after a failed Start, want 0", info.Delivered.Consumer)
	}
}

// With one handler slot and a handler that never returns, the consumer stops
// receiving once the first pull's messages are delivered, so the unacked
// count is the pull size in effect.
func TestConsume_PullMaxMessages(t *testing.T) {
	tests := []struct {
		name       string
		config     string
		subscriber string
		want       int
	}{
		{name: "nats.go default", want: 20},
		{name: "subscriber", subscriber: "3", want: 3},
		{name: "runtime config", config: "3", want: 3},
		{name: "subscriber over runtime config", config: "3", subscriber: "5", want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startServer(t)
			stream := createStream(t, url, "EVENTS", "test.event")
			cfg := testConfig(url)
			cfg[ConcurrencyKey] = "1"
			if tt.config != "" {
				cfg[PullMaxMessagesKey] = tt.config
			}
			md := map[string]string{StreamKey: "EVENTS"}
			if tt.subscriber != "" {
				md[PullMaxMessagesKey] = tt.subscriber
			}
			release := make(chan struct{})
			startRuntime(t, cfg, nil, []mesh.Subscriber{{Target: eventTarget, ConsumerGroup: "audit", Metadata: md, Handler: func(ctx context.Context, m mesh.Message) error {
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil
			}}})
			t.Cleanup(func() { close(release) }) // runs before Stop, so the drain finishes

			publishEvents(t, url, 20)
			waitFor(t, func() bool { return ackPending(t, stream, "audit") > 0 })
			time.Sleep(300 * time.Millisecond) // give deliveries past the pull size time to show up
			if got := ackPending(t, stream, "audit"); got != tt.want {
				t.Fatalf("%d messages unacked, want %d", got, tt.want)
			}
		})
	}
}
