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

func TestParseSettings(t *testing.T) {
	cases := []struct {
		name    string
		cfg     mesh.Config
		want    func(settings) bool
		wantErr error
	}{
		{
			name: "defaults",
			cfg:  mesh.Config{mesh.DeploymentGroupKey: "d"},
			want: func(s settings) bool {
				return s.connectTimeout == defaultConnectTimeout && s.requestTimeout == defaultRequestTimeout && s.concurrency > 0 && s.logger != nil
			},
		},
		{
			name: "all keys parsed",
			cfg: mesh.Config{
				mesh.DeploymentGroupKey: "d", URLKey: "nats://x:1", NameKey: "n",
				ConnectTimeoutKey: "1s", RequestTimeoutKey: "250ms", ConcurrencyKey: "3",
			},
			want: func(s settings) bool {
				return s.url == "nats://x:1" && s.name == "n" && s.connectTimeout == time.Second &&
					s.requestTimeout == 250*time.Millisecond && s.concurrency == 3 && s.deploymentGroup == "d"
			},
		},
		{name: "missing deployment group", cfg: mesh.Config{}, wantErr: mesh.ErrNoDeploymentGroup},
		{name: "empty deployment group", cfg: mesh.Config{mesh.DeploymentGroupKey: ""}, wantErr: mesh.ErrNoDeploymentGroup},
		{name: "bad duration", cfg: mesh.Config{mesh.DeploymentGroupKey: "d", RequestTimeoutKey: "soon"}, wantErr: ErrBadConfig},
		{name: "zero duration", cfg: mesh.Config{mesh.DeploymentGroupKey: "d", ConnectTimeoutKey: "0s"}, wantErr: ErrBadConfig},
		{name: "bad concurrency", cfg: mesh.Config{mesh.DeploymentGroupKey: "d", ConcurrencyKey: "many"}, wantErr: ErrBadConfig},
		{name: "zero concurrency", cfg: mesh.Config{mesh.DeploymentGroupKey: "d", ConcurrencyKey: "0"}, wantErr: ErrBadConfig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := parseSettings(tc.cfg, nil, true)
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

	t.Run("client does not require deployment group", func(t *testing.T) {
		if _, err := parseSettings(mesh.Config{}, nil, false); err != nil {
			t.Fatal(err)
		}
	})
}

func TestNew_KindMismatch(t *testing.T) {
	_, err := New(testConfig(""), mesh.ServiceMap{}, []mesh.Endpoint{{
		Target:  mesh.Target{Segments: []string{"a"}, Kind: mesh.KindTopic},
		Handler: okEndpoint,
	}}, nil)
	if !errors.Is(err, mesh.ErrKindMismatch) {
		t.Fatalf("endpoint on topic: want ErrKindMismatch, got %v", err)
	}

	_, err = New(testConfig(""), mesh.ServiceMap{}, nil, []mesh.Subscriber{{
		Target:  mesh.Target{Segments: []string{"a"}, Kind: mesh.KindRoute},
		Handler: okSubscriber,
	}})
	if !errors.Is(err, mesh.ErrKindMismatch) {
		t.Fatalf("subscriber on route: want ErrKindMismatch, got %v", err)
	}
}

func TestNew_InvalidTarget(t *testing.T) {
	_, err := New(testConfig(""), mesh.ServiceMap{}, []mesh.Endpoint{{
		Target:  mesh.Target{Segments: []string{"a.b"}, Kind: mesh.KindRoute},
		Handler: okEndpoint,
	}}, nil)
	if !errors.Is(err, mesh.ErrInvalidTarget) {
		t.Fatalf("want ErrInvalidTarget, got %v", err)
	}
}

func TestNew_DuplicateTarget(t *testing.T) {
	ep := mesh.Endpoint{Target: mesh.Target{Segments: []string{"a", "b"}, Kind: mesh.KindRoute}, Handler: okEndpoint}
	_, err := New(testConfig(""), mesh.ServiceMap{}, []mesh.Endpoint{ep, ep}, nil)
	if !errors.Is(err, ErrDuplicateTarget) {
		t.Fatalf("two endpoints: want ErrDuplicateTarget, got %v", err)
	}

	sub := mesh.Subscriber{Target: mesh.Target{Segments: []string{"a", "b"}, Kind: mesh.KindTopic}, Handler: okSubscriber}
	_, err = New(testConfig(""), mesh.ServiceMap{}, []mesh.Endpoint{ep}, []mesh.Subscriber{sub})
	if !errors.Is(err, ErrDuplicateTarget) {
		t.Fatalf("endpoint and subscriber on one subject: want ErrDuplicateTarget, got %v", err)
	}
}

func TestNew_NilHandler(t *testing.T) {
	_, err := New(testConfig(""), mesh.ServiceMap{}, []mesh.Endpoint{{
		Target: mesh.Target{Segments: []string{"a"}, Kind: mesh.KindRoute},
	}}, nil)
	if err == nil {
		t.Fatal("want error for nil handler")
	}
}

func TestNew_ServiceMapKept(t *testing.T) {
	sm := mesh.ServiceMap{Targets: []mesh.Target{echoTarget, eventTarget}}
	r, err := New(testConfig(""), sm, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.ServiceMap(); len(got.Targets) != 2 || !got.Targets[0].Equal(echoTarget) {
		t.Fatalf("ServiceMap not kept: %+v", got)
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
	cfg := mesh.Config{mesh.DeploymentGroupKey: "billing"}
	r, err := New(cfg, mesh.ServiceMap{},
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

func TestClient_ChecksBeforeConnection(t *testing.T) {
	s, _ := parseSettings(testConfig(""), nil, true)
	c := newSharedClient(s, mesh.ServiceMap{})
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

	_, err = c.Request(ctx, mesh.Message{Target: echoTarget}, nil)
	if !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Request with no connection: want ErrNotRunning, got %v", err)
	}
}
