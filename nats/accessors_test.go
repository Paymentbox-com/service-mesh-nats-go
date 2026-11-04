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

func TestRuntime_ReceiverForEndpoint(t *testing.T) {
	url := startServer(t)
	r := startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)

	rc, ok := r.Receiver(echoTarget)
	if !ok {
		t.Fatal("no receiver for the endpoint's target")
	}
	if !rc.Target().Equal(echoTarget) {
		t.Fatalf("receiver target %v, want %v", rc.Target(), echoTarget)
	}
}

func TestRuntime_ReceiverForTargetWithoutReceiver(t *testing.T) {
	url := startServer(t)
	r := startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)

	if _, ok := r.Receiver(eventTarget); ok {
		t.Fatal("want no receiver for a target the runtime does not serve")
	}
}

func TestReceiver_StreamFromOwnMetadata(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{StreamKey: "OTHER"}}
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:   target,
		Metadata: map[string]string{StreamKey: "EVENTS"},
		Handler:  okSubscriber,
	}})

	rc, ok := r.Receiver(target)
	if !ok {
		t.Fatal("no receiver for the target")
	}
	s, err := rc.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if name := s.CachedInfo().Config.Name; name != "EVENTS" {
		t.Fatalf("stream %q, want EVENTS", name)
	}
}

func TestReceiver_StreamWithoutStream(t *testing.T) {
	url := startServer(t)
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Handler: okSubscriber}})

	rc, ok := r.Receiver(eventTarget)
	if !ok {
		t.Fatal("no receiver for the target")
	}
	s, err := rc.Stream(context.Background())
	if s != nil || err != nil {
		t.Fatalf("want nil and no error, got %v, %v", s, err)
	}
}

func TestReceiver_Consumer(t *testing.T) {
	url := startServer(t)
	createStream(t, url, "EVENTS", "test.event")
	r := startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{
		Target:        eventTarget,
		ConsumerGroup: "audit",
		Metadata:      map[string]string{StreamKey: "EVENTS"},
		Handler:       okSubscriber,
	}})

	rc, ok := r.Receiver(eventTarget)
	if !ok {
		t.Fatal("no receiver for the target")
	}
	if name := rc.Consumer().CachedInfo().Name; name != "audit" {
		t.Fatalf("consumer %q, want audit", name)
	}
}

func TestRuntime_ReceiverBeforeStart(t *testing.T) {
	url := startServer(t)
	r := newRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)

	if _, ok := r.Receiver(echoTarget); ok {
		t.Fatal("want no receiver before Start")
	}
}

func TestRuntime_ReceiverAfterStop(t *testing.T) {
	url := startServer(t)
	r := startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, ok := r.Receiver(echoTarget); ok {
		t.Fatal("want no receiver after Stop")
	}
}
