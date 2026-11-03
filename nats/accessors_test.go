package nats

import (
	"context"
	"errors"
	"testing"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

func TestClient_StreamFromTarget(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	c := testClient(t, testConfig(url))

	target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{StreamKey: "EVENTS"}}
	s, err := c.Stream(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if name := s.CachedInfo().Config.Name; name != "EVENTS" {
		t.Fatalf("stream %q, want EVENTS", name)
	}
}

func TestClient_StreamForTargetWithoutStream(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))

	s, err := c.Stream(context.Background(), eventTarget)
	if s != nil || err != nil {
		t.Fatalf("want nil and no error, got %v, %v", s, err)
	}
}

func TestClient_StreamAfterClose(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	_ = c.Close()

	target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{StreamKey: "EVENTS"}}
	if _, err := c.Stream(context.Background(), target); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestRuntime_SubscriberForTargetWithoutSubscriber(t *testing.T) {
	url := startServer(t)
	r := startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)

	if _, ok := r.Subscriber(echoTarget); ok {
		t.Fatal("want no subscriber for an endpoint's target")
	}
}

func TestSubscriber_StreamFromOwnMetadata(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{StreamKey: "OTHER"}}
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:   target,
		Metadata: map[string]string{StreamKey: "EVENTS"},
		Handler:  okSubscriber,
	}})

	sub, ok := r.Subscriber(target)
	if !ok {
		t.Fatal("no subscriber for the target")
	}
	s, err := sub.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if name := s.CachedInfo().Config.Name; name != "EVENTS" {
		t.Fatalf("stream %q, want EVENTS", name)
	}
}

func TestSubscriber_StreamWithoutStream(t *testing.T) {
	url := startServer(t)
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Handler: okSubscriber}})

	sub, ok := r.Subscriber(eventTarget)
	if !ok {
		t.Fatal("no subscriber for the target")
	}
	s, err := sub.Stream(context.Background())
	if s != nil || err != nil {
		t.Fatalf("want nil and no error, got %v, %v", s, err)
	}
}

func TestSubscriber_Consumer(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:        eventTarget,
		ConsumerGroup: "audit",
		Metadata:      map[string]string{StreamKey: "EVENTS"},
		Handler:       okSubscriber,
	}})

	sub, ok := r.Subscriber(eventTarget)
	if !ok {
		t.Fatal("no subscriber for the target")
	}
	if name := sub.Consumer().CachedInfo().Name; name != "audit" {
		t.Fatalf("consumer %q, want audit", name)
	}
}
