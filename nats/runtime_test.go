package nats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsio "github.com/nats-io/nats.go"

	"github.com/Paymentbox-com/service-mesh-nats-go/mesh"
)

func TestRequest_RoundTrip(t *testing.T) {
	url := startServer(t)

	var seen mesh.Message
	echo := mesh.Endpoint{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		seen = m
		return mesh.Message{
			// A target the runtime must ignore: the reply goes to the requester.
			Target:   mesh.Target{Segments: []string{"nowhere"}, Kind: mesh.KindTopic},
			Metadata: map[string]string{"Reply-Key": "reply-value"},
			Payload:  bytes.ToUpper(m.Payload),
		}, nil
	}}
	startRuntime(t, testConfig(url), []mesh.Endpoint{echo}, nil)

	c := newClient(t, testConfig(url))
	reply, err := c.Request(context.Background(), mesh.Message{
		Target:   echoTarget,
		Metadata: map[string]string{"Request-Key": "request-value"},
		Payload:  []byte("hello"),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !seen.Target.Equal(echoTarget) {
		t.Errorf("handler saw target %v, want %v", seen.Target, echoTarget)
	}
	if seen.Metadata["Request-Key"] != "request-value" {
		t.Errorf("handler saw metadata %v", seen.Metadata)
	}
	if string(reply.Payload) != "HELLO" {
		t.Errorf("reply payload %q", reply.Payload)
	}
	if reply.Metadata["Reply-Key"] != "reply-value" {
		t.Errorf("reply metadata %v", reply.Metadata)
	}
	if !reply.Target.Equal(echoTarget) {
		t.Errorf("reply target %v, want the request target", reply.Target)
	}
}

func TestRequest_EmptyPayloadAndNoMetadata(t *testing.T) {
	url := startServer(t)
	startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)

	c := newClient(t, testConfig(url))
	reply, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Payload) != 0 || len(reply.Metadata) != 0 {
		t.Fatalf("want empty reply, got payload %q metadata %v", reply.Payload, reply.Metadata)
	}
}

func TestRequest_NoResponders(t *testing.T) {
	url := startServer(t)
	c := newClient(t, testConfig(url))

	_, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil)
	if !errors.Is(err, natsio.ErrNoResponders) {
		t.Fatalf("want natsio.ErrNoResponders, got %v", err)
	}
}

func TestRequest_HandlerError(t *testing.T) {
	url := startServer(t)
	failing := mesh.Target{Segments: []string{"test", "fail"}, Kind: mesh.KindRoute}
	panicking := mesh.Target{Segments: []string{"test", "panic"}, Kind: mesh.KindRoute}
	startRuntime(t, testConfig(url), []mesh.Endpoint{
		{Target: failing, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
			return mesh.Message{Payload: []byte("ignored")}, errors.New("boom")
		}},
		{Target: panicking, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
			panic("kaboom")
		}},
	}, nil, WithLogger(quietLogger()))
	c := newClient(t, testConfig(url))

	_, err := c.Request(context.Background(), mesh.Message{Target: failing}, nil)
	var he *HandlerError
	if !errors.As(err, &he) || he.Text != "boom" {
		t.Fatalf("returned error: want HandlerError{boom}, got %v", err)
	}

	_, err = c.Request(context.Background(), mesh.Message{Target: panicking}, nil)
	if !errors.As(err, &he) || !strings.Contains(he.Text, "kaboom") {
		t.Fatalf("panic: want HandlerError mentioning kaboom, got %v", err)
	}
}

func TestRequest_Timeouts(t *testing.T) {
	url := startServer(t)
	slow := mesh.Endpoint{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		select {
		case <-time.After(testWait):
		case <-ctx.Done():
		}
		return m, nil
	}}
	startRuntime(t, testConfig(url), []mesh.Endpoint{slow}, nil)

	cfg := testConfig(url)
	cfg[RequestTimeoutKey] = "100ms"
	c := newClient(t, cfg)

	_, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil)
	if !errors.Is(err, natsio.ErrTimeout) {
		t.Fatalf("configured timeout: want natsio.ErrTimeout, got %v", err)
	}

	start := time.Now()
	_, err = c.Request(context.Background(), mesh.Message{Target: echoTarget}, map[string]string{RequestTimeoutKey: "300ms"})
	if !errors.Is(err, natsio.ErrTimeout) || time.Since(start) < 250*time.Millisecond {
		t.Fatalf("per-call timeout: want natsio.ErrTimeout after ~300ms, got %v after %s", err, time.Since(start))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = c.Request(ctx, mesh.Message{Target: echoTarget}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline: want context.DeadlineExceeded, got %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	_, err = c.Request(ctx, mesh.Message{Target: echoTarget}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: want context.Canceled, got %v", err)
	}
}

func TestPublish_Subscriber(t *testing.T) {
	url := startServer(t)
	got := make(chan mesh.Message, 1)
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Handler: func(ctx context.Context, m mesh.Message) error {
		got <- m
		return nil
	}}})
	c := newClient(t, testConfig(url))

	err := c.Publish(context.Background(), mesh.Message{
		Target:   eventTarget,
		Metadata: map[string]string{"Event-Id": "42"},
		Payload:  []byte("created"),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if !m.Target.Equal(eventTarget) || string(m.Payload) != "created" || m.Metadata["Event-Id"] != "42" {
			t.Fatalf("subscriber saw %+v", m)
		}
	case <-time.After(testWait):
		t.Fatal("subscriber did not receive the message")
	}
}

func TestPublish_CancelledContext(t *testing.T) {
	url := startServer(t)
	c := newClient(t, testConfig(url))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Publish(ctx, mesh.Message{Target: eventTarget}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestPublish_ConsumerGroups(t *testing.T) {
	// Two runtimes, each with one subscriber, and one published message.
	none := map[string]string{mesh.ConsumerGroupKey: mesh.ConsumerGroupNone}
	shared := map[string]string{mesh.ConsumerGroupKey: "order-consumers"}
	cases := []struct {
		name          string
		groups        [2]string
		metadata      [2]map[string]string
		wantDelivered int32
	}{
		{name: "same deployment, key absent: one instance handles it", groups: [2]string{"billing", "billing"}, wantDelivered: 1},
		{name: "different deployments, key absent: each deployment handles it", groups: [2]string{"billing", "audit"}, wantDelivered: 2},
		{name: "same deployment, none: every instance handles it", groups: [2]string{"billing", "billing"}, metadata: [2]map[string]string{none, none}, wantDelivered: 2},
		{name: "different deployments, shared named group: one instance handles it", groups: [2]string{"billing", "audit"}, metadata: [2]map[string]string{shared, shared}, wantDelivered: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := startServer(t)
			var received atomic.Int32
			for i := 0; i < 2; i++ {
				sub := mesh.Subscriber{Target: eventTarget, Metadata: tc.metadata[i], Handler: func(ctx context.Context, m mesh.Message) error {
					received.Add(1)
					return nil
				}}
				startRuntime(t, mesh.Config{URLKey: url, mesh.DeploymentGroupKey: tc.groups[i]}, nil, []mesh.Subscriber{sub})
			}
			c := newClient(t, testConfig(url))

			if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, nil); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return received.Load() >= tc.wantDelivered })
			time.Sleep(100 * time.Millisecond) // give an unwanted extra delivery time to show up
			if got := received.Load(); got != tc.wantDelivered {
				t.Fatalf("want %d deliveries, got %d", tc.wantDelivered, got)
			}
		})
	}
}

func TestPublish_ConsumerGroupOnTarget(t *testing.T) {
	// The group set on the Target, not the binding, still separates the two.
	url := startServer(t)
	var received atomic.Int32
	for _, group := range []string{"a", "b"} {
		target := mesh.Target{Segments: eventTarget.Segments, Kind: mesh.KindTopic, Metadata: map[string]string{mesh.ConsumerGroupKey: group}}
		startRuntime(t, mesh.Config{URLKey: url, mesh.DeploymentGroupKey: "same"}, nil, []mesh.Subscriber{{Target: target, Handler: func(ctx context.Context, m mesh.Message) error {
			received.Add(1)
			return nil
		}}})
	}
	c := newClient(t, testConfig(url))
	if err := c.Publish(context.Background(), mesh.Message{Target: eventTarget}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return received.Load() >= 2 })
}

func TestRuntimeClient_NotRunning(t *testing.T) {
	url := startServer(t)
	r, err := New(testConfig(url), mesh.ServiceMap{}, []mesh.Endpoint{{Target: echoTarget, Handler: okEndpoint}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := r.Client()
	req := mesh.Message{Target: echoTarget}

	if _, err := c.Request(context.Background(), req, nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("before Start: want ErrNotRunning, got %v", err)
	}

	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Request(context.Background(), req, nil); err != nil {
		t.Fatalf("while running: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Request(context.Background(), req, nil); err != nil {
		t.Fatalf("Close on a shared client must not close the connection: %v", err)
	}

	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Request(context.Background(), req, nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("after Stop: want ErrNotRunning, got %v", err)
	}
}

func TestLifecycle(t *testing.T) {
	url := startServer(t)
	r, err := New(testConfig(url), mesh.ServiceMap{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if r.Running() {
		t.Fatal("Running before Start")
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("Stop before Start: want nil, got %v", err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !r.Running() {
		t.Fatal("not Running after Start")
	}
	if err := r.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start: want ErrAlreadyStarted, got %v", err)
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Running() {
		t.Fatal("Running after Stop")
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("second Stop: want nil, got %v", err)
	}
	if err := r.Start(ctx); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start after Stop: want ErrStopped, got %v", err)
	}
}

func TestStart_ConnectFailure(t *testing.T) {
	cfg := mesh.Config{URLKey: "nats://127.0.0.1:1", ConnectTimeoutKey: "200ms", mesh.DeploymentGroupKey: "test"}
	r, err := New(cfg, mesh.ServiceMap{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("want connect error")
	}
	if r.Running() {
		t.Fatal("Running after failed Start")
	}
}

func TestStop_DrainsInFlightHandler(t *testing.T) {
	url := startServer(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	r := startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		close(entered)
		<-release
		return mesh.Message{Payload: []byte("done")}, nil
	}}}, nil)
	c := newClient(t, testConfig(url))

	replies := make(chan error, 1)
	go func() {
		reply, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil)
		if err == nil && string(reply.Payload) != "done" {
			err = fmt.Errorf("payload %q", reply.Payload)
		}
		replies <- err
	}()
	<-entered

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		stopped <- r.Stop(ctx)
	}()

	select {
	case err := <-stopped:
		t.Fatalf("Stop returned %v before the handler finished", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-replies; err != nil {
		t.Fatalf("request during drain: %v", err)
	}
}

func TestStop_DeadlineCancelsHandlers(t *testing.T) {
	url := startServer(t)
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	r := startRuntime(t, testConfig(url), []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		return mesh.Message{}, ctx.Err()
	}}}, nil, WithLogger(quietLogger()))
	c := newClient(t, testConfig(url))

	go func() { _, _ = c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil) }()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := r.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop past deadline: want context.DeadlineExceeded, got %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(testWait):
		t.Fatal("handler context was not cancelled after Stop gave up")
	}
}

func TestConcurrencyBound(t *testing.T) {
	url := startServer(t)
	const limit = 2
	const requests = 6
	var inFlight, peak atomic.Int32
	cfg := testConfig(url)
	cfg[ConcurrencyKey] = "2"
	startRuntime(t, cfg, []mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		inFlight.Add(-1)
		return m, nil
	}}}, nil)
	c := newClient(t, testConfig(url))

	var wg sync.WaitGroup
	errs := make(chan error, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if p := peak.Load(); p > limit {
		t.Fatalf("peak concurrency %d exceeds limit %d", p, limit)
	}
	if p := peak.Load(); p < limit {
		t.Fatalf("peak concurrency %d never reached limit %d; the bound was not exercised", p, limit)
	}
}
