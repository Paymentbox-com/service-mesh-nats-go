package nats

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

func TestRequest_TargetRequestTimeout(t *testing.T) {
	url := startServer(t)
	release := make(chan struct{})
	startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		<-release
		return mesh.Message{}, nil
	}}}, nil)
	t.Cleanup(func() { close(release) })
	c := testClient(t, testConfig(url))

	target := mesh.Target{Segments: echoTarget.Segments, Kind: mesh.KindRoute, Metadata: map[string]string{RequestTimeoutKey: "200ms"}}
	start := time.Now()
	_, err := c.Request(context.Background(), mesh.Message{Target: target}, nil)
	if !errors.Is(err, natsio.ErrTimeout) || time.Since(start) > time.Second {
		t.Fatalf("want natsio.ErrTimeout after about 200ms, got %v after %s", err, time.Since(start))
	}
}

func TestRequest_OptionOutranksTarget(t *testing.T) {
	url := startServer(t)
	startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		time.Sleep(300 * time.Millisecond)
		return mesh.Message{}, nil
	}}}, nil)
	c := testClient(t, testConfig(url))

	target := mesh.Target{Segments: echoTarget.Segments, Kind: mesh.KindRoute, Metadata: map[string]string{RequestTimeoutKey: "100ms"}}
	if _, err := c.Request(context.Background(), mesh.Message{Target: target}, map[string]string{RequestTimeoutKey: "2s"}); err != nil {
		t.Fatalf("want the per-call 2s to apply, got %v", err)
	}
}

func TestSubscriber_MetadataOutranksTarget(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{StreamKey: "MISSING"}}
	got := make(chan mesh.Message, 1)
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:   target,
		Metadata: map[string]string{StreamKey: "EVENTS"},
		Handler: func(ctx context.Context, m mesh.Message) error {
			got <- m
			return nil
		},
	}})

	publishEvents(t, url, 1)
	select {
	case m := <-got:
		if m.Metadata[StreamMetadataKey] != "EVENTS" {
			t.Fatalf("delivered from stream %q, want EVENTS", m.Metadata[StreamMetadataKey])
		}
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
}

func TestNew_StreamOnEndpointTarget(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	target := mesh.Target{Segments: echoTarget.Segments, Kind: mesh.KindRoute, Metadata: map[string]string{StreamKey: "S"}}
	_, err := New(c, testConfig(""), []mesh.Endpoint{{Target: target, Handler: okEndpoint}}, nil)
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestRequest_StreamOnTarget(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	target := mesh.Target{Segments: echoTarget.Segments, Kind: mesh.KindRoute, Metadata: map[string]string{StreamKey: "S"}}
	_, err := c.Request(context.Background(), mesh.Message{Target: target}, nil)
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestPublish_TargetStream(t *testing.T) {
	// No stream exists, so a core publish succeeds and a JetStream publish
	// fails.
	url := startServer(t)
	c := testClient(t, testConfig(url))
	target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{StreamKey: "EVENTS"}}

	err := c.Publish(context.Background(), mesh.Message{Target: target}, nil)
	if !errors.Is(err, jetstream.ErrNoStreamResponse) {
		t.Fatalf("want jetstream.ErrNoStreamResponse, got %v", err)
	}
}

func TestSubscriber_StreamFromTarget(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{StreamKey: "EVENTS"}}
	got := make(chan mesh.Message, 1)
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: target, Handler: func(ctx context.Context, m mesh.Message) error {
		got <- m
		return nil
	}}})

	publishEvents(t, url, 1)
	select {
	case m := <-got:
		if m.Metadata[StreamMetadataKey] != "EVENTS" {
			t.Fatalf("delivered from stream %q, want EVENTS", m.Metadata[StreamMetadataKey])
		}
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
}

func TestSubscriber_FailureLogged(t *testing.T) {
	url := startServer(t)
	logs := &logRecords{}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Handler: func(ctx context.Context, m mesh.Message) error {
		return errors.New("boom")
	}}}, WithLogger(slog.New(logs)))
	c := testClient(t, testConfig(url))

	if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(logs.withMessage("nats: subscriber handler failed")) == 1 })
	rec := logs.withMessage("nats: subscriber handler failed")[0]
	if rec["subject"].String() != "test.event" || rec["error"].String() != "boom" {
		t.Fatalf("log record %v, want subject test.event and error boom", rec)
	}
}
