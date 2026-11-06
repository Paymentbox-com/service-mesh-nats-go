// Command send makes requests or publishes with the settings given as flags,
// and prints each result. It is the sending side of the local behavior
// runbook in docs/runbooks/local-behavior.md.
//
//	go run ./docs/runbooks/cmd/send -kind topic -subject demo.created -set stream=DEMO_CREATED -n 3
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
	"github.com/Paymentbox-com/service-mesh-nats-go/nats"
)

// pairs collects repeated key=value flags.
type pairs map[string]string

func (p pairs) String() string { return fmt.Sprint(map[string]string(p)) }

func (p pairs) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("want key=value, got %q", v)
	}
	p[k] = val
	return nil
}

func main() {
	cfg, set, target, md := pairs{}, pairs{}, pairs{}, pairs{}
	kind := flag.String("kind", "topic", "route to Request, topic to Publish")
	subj := flag.String("subject", "demo.created", "the target's subject, segments joined with dots")
	n := flag.Int("n", 1, "how many messages to send")
	payload := flag.String("payload", "hello", "the payload; each message appends its number")
	deadline := flag.Duration("deadline", 0, "a context deadline for each call, 0 for none")
	streamInfo := flag.Bool("stream-info", false, "after sending, print the target's stream from Client.Stream")
	flag.Var(cfg, "cfg", "a Config value, key=value, repeatable")
	flag.Var(set, "set", "a per-call option, key=value, repeatable")
	flag.Var(target, "target", "a Target metadata setting, key=value, repeatable")
	flag.Var(md, "md", "a message metadata key, key=value, repeatable")
	flag.Parse()

	if os.Getenv("NATS_URL") != "" {
		cfg[nats.URLKey] = os.Getenv("NATS_URL")
	}

	t := mesh.Target{Segments: strings.Split(*subj, "."), Kind: mesh.KindTopic, Metadata: target}
	if *kind == "route" {
		t.Kind = mesh.KindRoute
	}

	client, err := nats.NewClient(mesh.Config(cfg), mesh.ServiceMap{Targets: []mesh.Target{t}})
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	for i := 1; i <= *n; i++ {
		ctx := context.Background()
		if *deadline > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, *deadline)
			defer cancel()
		}
		msg := mesh.Message{Target: t, Metadata: md, Payload: []byte(fmt.Sprintf("%s %d", *payload, i))}
		start := time.Now()
		if t.Kind == mesh.KindRoute {
			reply, err := client.Request(ctx, msg, set)
			if err != nil {
				log.Printf("request %d: %s after %s", i, describe(err), time.Since(start).Round(time.Millisecond))
				continue
			}
			log.Printf("request %d: reply %q metadata %v after %s", i, reply.Payload, reply.Metadata, time.Since(start).Round(time.Millisecond))
			continue
		}
		if err := client.Publish(ctx, msg, set); err != nil {
			log.Printf("publish %d: %s after %s", i, describe(err), time.Since(start).Round(time.Millisecond))
			continue
		}
		log.Printf("publish %d: ok after %s", i, time.Since(start).Round(time.Millisecond))
	}

	if *streamInfo {
		s, err := client.Stream(context.Background(), t)
		switch {
		case err != nil:
			log.Printf("Client.Stream: %v", err)
		case s == nil:
			log.Print("Client.Stream: the target has no stream setting")
		default:
			info, err := s.Info(context.Background())
			if err != nil {
				log.Printf("stream info: %v", err)
				return
			}
			log.Printf("stream %q: %d messages, last sequence %d, %d consumers", info.Config.Name, info.State.Msgs, info.State.LastSeq, info.State.Consumers)
		}
	}
}

// describe names the error so each runbook step has a recognizable outcome.
func describe(err error) string {
	var he *nats.HandlerError
	switch {
	case errors.As(err, &he):
		return fmt.Sprintf("HandlerError %q", he.Text)
	case errors.Is(err, nats.ErrTimeout):
		return "nats.ErrTimeout"
	case errors.Is(err, nats.ErrNoResponders):
		return "nats.ErrNoResponders"
	case errors.Is(err, jetstream.ErrNoStreamResponse):
		return "jetstream.ErrNoStreamResponse"
	case errors.Is(err, nats.ErrBadConfig):
		return fmt.Sprintf("nats.ErrBadConfig: %v", err)
	case errors.Is(err, context.DeadlineExceeded):
		return "context.DeadlineExceeded"
	default:
		return fmt.Sprintf("%T: %v", err, err)
	}
}
