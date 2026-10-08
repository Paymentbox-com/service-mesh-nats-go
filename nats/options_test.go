package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
)

// newWithSubscriberMetadata builds a runtime with one subscriber on
// eventTarget carrying md, and returns New's error.
func newWithSubscriberMetadata(t *testing.T, md map[string]string) error {
	t.Helper()
	c := testClient(t, testConfig(startServer(t)))
	_, err := New(c, testConfig(""), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: okSubscriber}})
	return err
}

func TestNew_StreamOnEndpoint(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	_, err := New(c, testConfig(""), []mesh.Endpoint{{Target: echoTarget, Metadata: map[string]string{StreamKey: "S"}, Handler: okEndpoint}}, nil)
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_CreateConsumerFalseWithoutGroup(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", CreateConsumerKey: "false", mesh.ConsumerGroupKey: mesh.ConsumerGroupNone})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadCreateConsumer(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", CreateConsumerKey: "no"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadAckPolicy(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", AckPolicyKey: "all"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadAckWait(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", AckWaitKey: "120"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadMaxDeliver(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", MaxDeliverKey: "0"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadMaxDeliverConfig(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	cfg := testConfig("")
	cfg[MaxDeliverKey] = "many"
	if _, err := New(c, cfg, nil, nil); !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadBackoff(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", BackoffKey: "1s,soon"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BackoffNotShorterThanMaxDeliver(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", BackoffKey: "1s,2s,3s", MaxDeliverKey: "3"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadNakDelay(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", NakDelayKey: "-1s"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadMaxAckPending(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", MaxAckPendingKey: "lots"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadDeliverPolicy(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", DeliverPolicyKey: "last"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestNew_BadConsumerInactiveThreshold(t *testing.T) {
	err := newWithSubscriberMetadata(t, map[string]string{StreamKey: "S", ConsumerInactiveThresholdKey: "7d"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestRequest_StreamOption(t *testing.T) {
	c := testClient(t, testConfig(startServer(t)))
	_, err := c.Request(context.Background(), mesh.Message{Target: echoTarget}, map[string]string{StreamKey: "S"})
	if !errors.Is(err, ErrBadConfig) {
		t.Fatalf("want ErrBadConfig, got %v", err)
	}
}

func TestStart_SubscriberWithEveryOption(t *testing.T) {
	url := startServer(t)
	stream := createStream(t, url, "EVENTS", "test.event")
	md := map[string]string{
		mesh.ConsumerGroupKey:        "audit",
		StreamKey:                    "EVENTS",
		CreateConsumerKey:            "true",
		AckPolicyKey:                 "explicit",
		AckWaitKey:                   "10s",
		MaxDeliverKey:                "4",
		BackoffKey:                   "1s, 2s,3s",
		NakDelayKey:                  "1s",
		MaxAckPendingKey:             "20",
		DeliverPolicyKey:             "new",
		ConsumerInactiveThresholdKey: "1h",
	}
	startRuntime(t, testConfig(url), nil, []mesh.Subscriber{{Target: eventTarget, Metadata: md, Handler: okSubscriber}})

	cons, err := stream.Consumer(context.Background(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	cfg := cons.CachedInfo().Config
	if cfg.FilterSubject != "test.event" ||
		cfg.AckPolicy != jetstream.AckExplicitPolicy ||
		cfg.MaxDeliver != 4 ||
		len(cfg.BackOff) != 3 || cfg.BackOff[1] != 2*time.Second ||
		cfg.MaxAckPending != 20 ||
		cfg.DeliverPolicy != jetstream.DeliverNewPolicy ||
		cfg.InactiveThreshold != time.Hour {
		t.Fatalf("consumer settings %+v", cfg)
	}
}
