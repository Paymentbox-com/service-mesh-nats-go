package nats

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

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
// NewClient reads the url and New the deployment group.
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
	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
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

// startRuntime builds a client from cfg, a runtime on it with opts, and
// starts it. Stop at cleanup closes the client.
func startRuntime(t *testing.T, cfg mesh.Config, endpoints []mesh.Endpoint, subscribers []mesh.Subscriber, opts ...Option) *Runtime {
	t.Helper()
	c, err := NewClient(cfg, mesh.ServiceMap{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(c, cfg, endpoints, subscribers, opts...)
	if err != nil {
		_ = c.Close()
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

// testClient is a connected standalone client closed at cleanup.
func testClient(t *testing.T, cfg mesh.Config) *Client {
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

// testJetStream is a JetStream context on its own connection, closed at
// cleanup, for setting up and inspecting streams and consumers.
func testJetStream(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := natsio.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// createStream creates a stream named name holding subjects.
func createStream(t *testing.T, url, name string, subjects ...string) jetstream.Stream {
	t.Helper()
	s, err := testJetStream(t, url).CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: subjects})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// streamMessages is how many messages the stream holds.
func streamMessages(t *testing.T, s jetstream.Stream) uint64 {
	t.Helper()
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

// newRuntime builds a client from cfg and a runtime on it, without starting
// it. The client is closed at cleanup.
func newRuntime(t *testing.T, cfg mesh.Config, endpoints []mesh.Endpoint, subscribers []mesh.Subscriber, opts ...Option) *Runtime {
	t.Helper()
	c := testClient(t, cfg)
	r, err := New(c, cfg, endpoints, subscribers, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// logRecords collects what a runtime logs.
type logRecords struct {
	mu      sync.Mutex
	records []slog.Record
}

func (l *logRecords) Enabled(context.Context, slog.Level) bool { return true }
func (l *logRecords) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logRecords) WithGroup(string) slog.Handler            { return l }

func (l *logRecords) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r)
	return nil
}

// withMessage returns the attributes of every record whose message is msg.
func (l *logRecords) withMessage(msg string) []map[string]slog.Value {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]slog.Value
	for _, r := range l.records {
		if r.Message != msg {
			continue
		}
		attrs := map[string]slog.Value{}
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value
			return true
		})
		out = append(out, attrs)
	}
	return out
}
