package nats

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

func TestRequest_HandlerSeesTimeoutAndDeadline(t *testing.T) {
	url := startServer(t)
	seen := make(chan map[string]string, 1)
	startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		seen <- m.Metadata
		return mesh.Message{}, nil
	}}}, nil)
	cfg := testConfig(url)
	cfg[RequestTimeoutKey] = "2s"
	c := testClient(t, cfg)

	sent := time.Now()
	if _, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil); err != nil {
		t.Fatal(err)
	}
	md := <-seen

	ms, err := strconv.Atoi(md[mesh.TimeoutKey])
	if err != nil || ms <= 1900 || ms > 2000 {
		t.Fatalf("Mesh-Timeout %q, want about 2000", md[mesh.TimeoutKey])
	}
	deadline, err := time.Parse(time.RFC3339Nano, md[mesh.DeadlineKey])
	if err != nil {
		t.Fatalf("Mesh-Deadline %q: %v", md[mesh.DeadlineKey], err)
	}
	if want := sent.Add(2 * time.Second); deadline.Before(want.Add(-100*time.Millisecond)) || deadline.After(want.Add(100*time.Millisecond)) {
		t.Fatalf("Mesh-Deadline %s, want about %s", deadline, want)
	}
}

func TestRequest_TimeoutFromSoonerContextDeadline(t *testing.T) {
	url := startServer(t)
	seen := make(chan map[string]string, 1)
	startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		seen <- m.Metadata
		return mesh.Message{}, nil
	}}}, nil)
	c := testClient(t, testConfig(url))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Request(ctx, mesh.Message{Target: echoTarget}, nil); err != nil {
		t.Fatal(err)
	}
	md := <-seen
	if ms, err := strconv.Atoi(md[mesh.TimeoutKey]); err != nil || ms <= 400 || ms > 500 {
		t.Fatalf("Mesh-Timeout %q, want about 500", md[mesh.TimeoutKey])
	}
}

func TestRequest_RequestTimeoutBeforeLaterContextDeadline(t *testing.T) {
	url := startServer(t)
	startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		<-ctx.Done()
		return mesh.Message{}, nil
	}}}, nil)
	cfg := testConfig(url)
	cfg[RequestTimeoutKey] = "200ms"
	c := testClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	start := time.Now()
	_, err := c.Request(ctx, mesh.Message{Target: echoTarget}, nil)
	if !errors.Is(err, natsio.ErrTimeout) || time.Since(start) > time.Second {
		t.Fatalf("want natsio.ErrTimeout after about 200ms, got %v after %s", err, time.Since(start))
	}
}

func TestEndpoint_ContextCarriesDeadline(t *testing.T) {
	url := startServer(t)
	type seenRequest struct {
		md       map[string]string
		deadline time.Time
		ok       bool
	}
	seen := make(chan seenRequest, 1)
	startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		d, ok := ctx.Deadline()
		seen <- seenRequest{md: m.Metadata, deadline: d, ok: ok}
		return mesh.Message{}, nil
	}}}, nil)
	c := testClient(t, testConfig(url))

	if _, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil); err != nil {
		t.Fatal(err)
	}
	got := <-seen
	if !got.ok {
		t.Fatal("handler context has no deadline")
	}
	if got.deadline.Format(time.RFC3339Nano) != got.md[mesh.DeadlineKey] {
		t.Fatalf("context deadline %s, Mesh-Deadline %s", got.deadline.Format(time.RFC3339Nano), got.md[mesh.DeadlineKey])
	}
}

func TestRequest_MessageIDSetsNatsMsgID(t *testing.T) {
	url := startServer(t)
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	headers := make(chan natsio.Header, 1)
	if _, err := nc.Subscribe("test.echo", func(m *natsio.Msg) {
		headers <- m.Header
		_ = m.Respond(nil)
	}); err != nil {
		t.Fatal(err)
	}
	c := testClient(t, testConfig(url))

	msg := mesh.Message{Target: echoTarget, Metadata: map[string]string{mesh.MessageIDKey: "id-1"}}
	if _, err := c.Request(context.Background(), msg, nil); err != nil {
		t.Fatal(err)
	}
	if got := (<-headers).Get("Nats-Msg-Id"); got != "id-1" {
		t.Fatalf("Nats-Msg-Id %q, want id-1", got)
	}
}

func TestPublish_MessageIDSetsNatsMsgID(t *testing.T) {
	url := startServer(t)
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	sub, err := nc.SubscribeSync("test.event")
	if err != nil {
		t.Fatal(err)
	}
	c := testClient(t, testConfig(url))

	msg := mesh.Message{Target: eventTarget, Metadata: map[string]string{mesh.MessageIDKey: "id-1"}}
	if err := c.Publish(context.Background(), msg, nil); err != nil {
		t.Fatal(err)
	}
	m, err := sub.NextMsg(testWait)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Header.Get("Nats-Msg-Id"); got != "id-1" {
		t.Fatalf("Nats-Msg-Id %q, want id-1", got)
	}
}

func TestSubscriber_NatsMsgIDSetsMessageID(t *testing.T) {
	url := startServer(t)
	seen := make(chan map[string]string, 1)
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Handler: func(ctx context.Context, m mesh.Message) error {
		seen <- m.Metadata
		return nil
	}}})
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	msg := natsio.NewMsg("test.event")
	msg.Header.Set("Nats-Msg-Id", "native-id")
	msg.Header.Set(mesh.MessageIDKey, "stale-id")
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
	select {
	case md := <-seen:
		if md[mesh.MessageIDKey] != "native-id" {
			t.Fatalf("Mesh-Message-Id %q, want native-id", md[mesh.MessageIDKey])
		}
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
}

func TestPublish_MessageIDWinsOverNatsMsgID(t *testing.T) {
	url := startServer(t)
	seen := make(chan map[string]string, 1)
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Handler: func(ctx context.Context, m mesh.Message) error {
		seen <- m.Metadata
		return nil
	}}})
	c := testClient(t, testConfig(url))

	msg := mesh.Message{Target: eventTarget, Metadata: map[string]string{
		mesh.MessageIDKey: "mesh-id",
		"Nats-Msg-Id":     "native-id",
	}}
	if err := c.Publish(context.Background(), msg, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case md := <-seen:
		if md[mesh.MessageIDKey] != "mesh-id" || md["Nats-Msg-Id"] != "mesh-id" {
			t.Fatalf("Mesh-Message-Id %q and Nats-Msg-Id %q, want mesh-id in both", md[mesh.MessageIDKey], md["Nats-Msg-Id"])
		}
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
}
