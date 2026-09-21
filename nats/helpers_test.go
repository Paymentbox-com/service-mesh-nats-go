package nats

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

const testWait = 3 * time.Second

var (
	echoTarget  = mesh.Target{Segments: []string{"test", "echo"}, Kind: mesh.KindRoute}
	eventTarget = mesh.Target{Segments: []string{"test", "event"}, Kind: mesh.KindTopic}
)

func okEndpoint(ctx context.Context, m mesh.Message) (mesh.Message, error) { return m, nil }
func okSubscriber(ctx context.Context, m mesh.Message) error               { return nil }

// testConfig is the smallest Config New accepts, with url added when set.
func testConfig(url string) mesh.Config {
	cfg := mesh.Config{mesh.DeploymentGroupKey: "test"}
	if url != "" {
		cfg[URLKey] = url
	}
	return cfg
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func startServer(t *testing.T) string {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(testWait) {
		t.Fatal("nats-server did not become ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

func startRuntime(t *testing.T, cfg mesh.Config, endpoints []mesh.Endpoint, subscribers []mesh.Subscriber, opts ...Option) *Runtime {
	t.Helper()
	r, err := New(cfg, mesh.ServiceMap{}, endpoints, subscribers, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		_ = r.Stop(ctx)
	})
	return r
}

func newClient(t *testing.T, cfg mesh.Config) *Client {
	t.Helper()
	c, err := NewClient(cfg, mesh.ServiceMap{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
