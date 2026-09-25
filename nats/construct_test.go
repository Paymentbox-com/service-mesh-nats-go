package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

func TestSubject(t *testing.T) {
	cases := []struct {
		name     string
		segments []string
		want     string
		invalid  bool
	}{
		{name: "several segments", segments: []string{"orders", "create"}, want: "orders.create"},
		{name: "single segment", segments: []string{"ping"}, want: "ping"},
		{name: "unicode printable", segments: []string{"héllo"}, want: "héllo"},
		{name: "no segments", segments: nil, invalid: true},
		{name: "empty segment", segments: []string{"a", ""}, invalid: true},
		{name: "dot in segment", segments: []string{"a.b"}, invalid: true},
		{name: "star wildcard", segments: []string{"*"}, invalid: true},
		{name: "gt wildcard", segments: []string{"a", ">"}, invalid: true},
		{name: "whitespace", segments: []string{"a b"}, invalid: true},
		{name: "non-printable", segments: []string{"a\x01"}, invalid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := subject(mesh.Target{Segments: tc.segments, Kind: mesh.KindRoute})
			if tc.invalid {
				if !errors.Is(err, mesh.ErrInvalidTarget) {
					t.Fatalf("want ErrInvalidTarget, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("want %q, got %q", tc.want, got)
			}
		})
	}
}

func TestParseRuntimeSettings(t *testing.T) {
	cases := []struct {
		name    string
		cfg     mesh.Config
		want    func(runtimeSettings) bool
		wantErr error
	}{
		{
			name: "defaults",
			cfg:  mesh.Config{mesh.DeploymentGroupKey: "d"},
			want: func(s runtimeSettings) bool {
				return s.deploymentGroup == "d" && s.concurrency > 0 && s.logger != nil
			},
		},
		{
			name: "all keys parsed",
			cfg:  mesh.Config{mesh.DeploymentGroupKey: "d", ConcurrencyKey: "3"},
			want: func(s runtimeSettings) bool { return s.deploymentGroup == "d" && s.concurrency == 3 },
		},
		{
			name: "connection keys ignored",
			cfg:  mesh.Config{mesh.DeploymentGroupKey: "d", URLKey: "nats://x:1", ConnectTimeoutKey: "soon", RequestTimeoutKey: "later"},
			want: func(s runtimeSettings) bool { return s.deploymentGroup == "d" },
		},
		{name: "missing deployment group", cfg: mesh.Config{}, wantErr: mesh.ErrNoDeploymentGroup},
		{name: "empty deployment group", cfg: mesh.Config{mesh.DeploymentGroupKey: ""}, wantErr: mesh.ErrNoDeploymentGroup},
		{name: "bad concurrency", cfg: mesh.Config{mesh.DeploymentGroupKey: "d", ConcurrencyKey: "many"}, wantErr: ErrBadConfig},
		{name: "zero concurrency", cfg: mesh.Config{mesh.DeploymentGroupKey: "d", ConcurrencyKey: "0"}, wantErr: ErrBadConfig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := parseRuntimeSettings(tc.cfg, nil)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !tc.want(s) {
				t.Fatalf("settings not as expected: %+v", s)
			}
		})
	}
}

func TestParseClientSettings(t *testing.T) {
	cases := []struct {
		name    string
		cfg     mesh.Config
		want    func(clientSettings) bool
		wantErr error
	}{
		{
			name: "defaults",
			cfg:  mesh.Config{},
			want: func(s clientSettings) bool {
				return s.connectTimeout == defaultConnectTimeout && s.requestTimeout == defaultRequestTimeout && s.url != ""
			},
		},
		{
			name: "all keys parsed",
			cfg:  mesh.Config{URLKey: "nats://x:1", NameKey: "n", ConnectTimeoutKey: "1s", RequestTimeoutKey: "250ms"},
			want: func(s clientSettings) bool {
				return s.url == "nats://x:1" && s.name == "n" && s.connectTimeout == time.Second && s.requestTimeout == 250*time.Millisecond
			},
		},
		{
			name: "runtime keys ignored",
			cfg:  mesh.Config{mesh.DeploymentGroupKey: "d", ConcurrencyKey: "many"},
			want: func(s clientSettings) bool { return s.requestTimeout == defaultRequestTimeout },
		},
		{name: "bad duration", cfg: mesh.Config{RequestTimeoutKey: "soon"}, wantErr: ErrBadConfig},
		{name: "zero duration", cfg: mesh.Config{ConnectTimeoutKey: "0s"}, wantErr: ErrBadConfig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := parseClientSettings(tc.cfg)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !tc.want(s) {
				t.Fatalf("settings not as expected: %+v", s)
			}
		})
	}
}

func TestNew_NilClient(t *testing.T) {
	_, err := New(nil, testConfig(""), nil, nil)
	if err == nil {
		t.Fatal("want error for nil client")
	}
}

func TestNew_KindMismatch(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	_, err := New(c, testConfig(""), []mesh.Endpoint{{
		Target:  mesh.Target{Segments: []string{"a"}, Kind: mesh.KindTopic},
		Handler: okEndpoint,
	}}, nil)
	if !errors.Is(err, mesh.ErrKindMismatch) {
		t.Fatalf("endpoint on topic: want ErrKindMismatch, got %v", err)
	}

	_, err = New(c, testConfig(""), nil, []mesh.Subscriber{{
		Target:  mesh.Target{Segments: []string{"a"}, Kind: mesh.KindRoute},
		Handler: okSubscriber,
	}})
	if !errors.Is(err, mesh.ErrKindMismatch) {
		t.Fatalf("subscriber on route: want ErrKindMismatch, got %v", err)
	}
}

func TestNew_InvalidTarget(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	_, err := New(c, testConfig(""), []mesh.Endpoint{{
		Target:  mesh.Target{Segments: []string{"a.b"}, Kind: mesh.KindRoute},
		Handler: okEndpoint,
	}}, nil)
	if !errors.Is(err, mesh.ErrInvalidTarget) {
		t.Fatalf("want ErrInvalidTarget, got %v", err)
	}
}

func TestNew_NilHandler(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	_, err := New(c, testConfig(""), []mesh.Endpoint{{
		Target: mesh.Target{Segments: []string{"a"}, Kind: mesh.KindRoute},
	}}, nil)
	if err == nil {
		t.Fatal("want error for nil handler")
	}
}

func TestRuntime_ServiceMapIsClients(t *testing.T) {
	url := startServer(t)
	sm := mesh.ServiceMap{Targets: []mesh.Target{echoTarget, eventTarget}}
	c, err := NewClient(testConfig(url), sm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	r, err := New(c, testConfig(""), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.ServiceMap(); len(got.Targets) != 2 || !got.Targets[0].Equal(echoTarget) {
		t.Fatalf("ServiceMap is not the client's: %+v", got)
	}
}

func TestNewClient_ServiceMapKept(t *testing.T) {
	url := startServer(t)
	sm := mesh.ServiceMap{Targets: []mesh.Target{echoTarget, eventTarget}}
	c, err := NewClient(testConfig(url), sm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if got := c.ServiceMap(); len(got.Targets) != 2 || !got.Targets[0].Equal(echoTarget) {
		t.Fatalf("ServiceMap not kept: %+v", got)
	}
}

func TestConsumerGroupResolution(t *testing.T) {
	none := map[string]string{mesh.ConsumerGroupKey: mesh.ConsumerGroupNone}
	named := func(n string) map[string]string { return map[string]string{mesh.ConsumerGroupKey: n} }
	cases := []struct {
		name    string
		binding map[string]string
		target  map[string]string
		want    string
	}{
		{name: "absent everywhere joins deployment group", want: "billing"},
		{name: "none on binding gives no group", binding: none, want: ""},
		{name: "name on binding", binding: named("audit"), want: "audit"},
		{name: "name on target", target: named("audit"), want: "audit"},
		{name: "none on target", target: none, want: ""},
		{name: "binding wins over target", binding: named("from-binding"), target: named("from-target"), want: "from-binding"},
		{name: "empty binding value falls through to target", binding: named(""), target: named("audit"), want: "audit"},
		{name: "empty everywhere falls through to deployment group", binding: named(""), target: named(""), want: "billing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := consumerGroup(tc.binding, tc.target, "billing"); got != tc.want {
				t.Fatalf("want %q, got %q", tc.want, got)
			}
		})
	}
}

func TestNew_BindingsUseConsumerGroup(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	cfg := mesh.Config{mesh.DeploymentGroupKey: "billing"}
	r, err := New(c, cfg,
		[]mesh.Endpoint{{Target: echoTarget, Metadata: map[string]string{mesh.ConsumerGroupKey: mesh.ConsumerGroupNone}, Handler: okEndpoint}},
		[]mesh.Subscriber{{Target: eventTarget, Handler: okSubscriber}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.bindings[0].queue; got != "" {
		t.Fatalf("endpoint with none: want plain subscription, got queue %q", got)
	}
	if got := r.bindings[1].queue; got != "billing" {
		t.Fatalf("subscriber default: want %q, got %q", "billing", got)
	}
}

func TestClient_ChecksBeforeWire(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	ctx := context.Background()

	_, err := c.Request(ctx, mesh.Message{Target: mesh.Target{Segments: []string{"a"}, Kind: mesh.KindTopic}}, nil)
	if !errors.Is(err, mesh.ErrKindMismatch) {
		t.Fatalf("Request with topic: want ErrKindMismatch, got %v", err)
	}

	err = c.Publish(ctx, mesh.Message{Target: mesh.Target{Segments: []string{"a"}, Kind: mesh.KindRoute}}, nil)
	if !errors.Is(err, mesh.ErrKindMismatch) {
		t.Fatalf("Publish with route: want ErrKindMismatch, got %v", err)
	}

	_, err = c.Request(ctx, mesh.Message{Target: mesh.Target{Segments: []string{"a b"}, Kind: mesh.KindRoute}}, nil)
	if !errors.Is(err, mesh.ErrInvalidTarget) {
		t.Fatalf("Request with bad segment: want ErrInvalidTarget, got %v", err)
	}

	_, err = c.Request(ctx, mesh.Message{Target: echoTarget}, map[string]string{RequestTimeoutKey: "later"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("Request with bad option: want ErrBadConfig, got %v", err)
	}
}

func TestClient_CloseTwice(t *testing.T) {
	url := startServer(t)
	c, err := NewClient(testConfig(url), mesh.ServiceMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestClient_RequestAfterClose(t *testing.T) {
	url := startServer(t)
	c, err := NewClient(testConfig(url), mesh.ServiceMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = c.Request(context.Background(), mesh.Message{Target: echoTarget}, nil)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}
