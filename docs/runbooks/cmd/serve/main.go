// Command serve runs one endpoint or subscriber with the settings given as
// flags, and prints every message its handler receives. It is the receiving
// side of the local behavior runbook in docs/runbooks/local-behavior.md.
//
//	go run ./docs/runbooks/cmd/serve -kind topic -subject demo.created -set stream=DEMO_CREATED
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

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
	cfg, set, target := pairs{}, pairs{}, pairs{}
	kind := flag.String("kind", "topic", "route for an endpoint, topic for a subscriber")
	subj := flag.String("subject", "demo.created", "the target's subject, segments joined with dots")
	group := flag.String("group", "", "ConsumerGroup: empty for the deployment group, none, or a name")
	fail := flag.Int("fail", 0, "fail the first N handler calls")
	work := flag.Duration("work", 0, "how long each handler call runs before it returns")
	drain := flag.Duration("drain", 10*time.Second, "the drain budget Stop gets on Ctrl-C")
	flag.Var(cfg, "cfg", "a Config value, key=value, repeatable")
	flag.Var(set, "set", "an Endpoint or Subscriber metadata setting, key=value, repeatable")
	flag.Var(target, "target", "a Target metadata setting, key=value, repeatable")
	flag.Parse()

	if os.Getenv("NATS_URL") != "" {
		cfg[nats.URLKey] = os.Getenv("NATS_URL")
	}
	if _, ok := cfg[mesh.DeploymentGroupKey]; !ok {
		cfg[mesh.DeploymentGroupKey] = "demo"
	}

	t := mesh.Target{Segments: strings.Split(*subj, "."), Kind: mesh.KindTopic, Metadata: target}
	if *kind == "route" {
		t.Kind = mesh.KindRoute
	}

	var calls atomic.Int32
	handle := func(ctx context.Context, m mesh.Message) error {
		n := calls.Add(1)
		printMessage(n, ctx, m)
		if *work > 0 {
			select {
			case <-time.After(*work):
			case <-ctx.Done():
				log.Printf("call %d: context done: %v", n, ctx.Err())
			}
		}
		if int(n) <= *fail {
			log.Printf("call %d: returning an error", n)
			return fmt.Errorf("failing call %d of %d on purpose", n, *fail)
		}
		return nil
	}

	var endpoints []mesh.Endpoint
	var subscribers []mesh.Subscriber
	if t.Kind == mesh.KindRoute {
		endpoints = append(endpoints, mesh.Endpoint{Target: t, ConsumerGroup: *group, Metadata: set, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
			if err := handle(ctx, m); err != nil {
				return mesh.Message{}, err
			}
			return mesh.Message{Metadata: map[string]string{"Served-By": fmt.Sprint(os.Getpid())}, Payload: append([]byte("echo: "), m.Payload...)}, nil
		}})
	} else {
		subscribers = append(subscribers, mesh.Subscriber{Target: t, ConsumerGroup: *group, Metadata: set, Handler: handle})
	}

	client, err := nats.NewClient(mesh.Config(cfg), mesh.ServiceMap{Targets: []mesh.Target{t}})
	if err != nil {
		log.Fatalf("NewClient: %v", err)
	}
	rt, err := nats.New(client, mesh.Config(cfg), endpoints, subscribers)
	if err != nil {
		_ = client.Close()
		log.Fatalf("New: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		_ = client.Close()
		log.Fatalf("Start: %v", err)
	}
	printReceiver(rt, t)
	log.Printf("serving %s %s (pid %d); Ctrl-C to stop", *kind, *subj, os.Getpid())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()
	log.Printf("stopping with a %s drain", *drain)
	if err := rt.Stop(ctx); err != nil {
		log.Printf("Stop: %v", err)
		return
	}
	log.Print("stopped")
}

// printMessage prints one delivery: its payload, its metadata sorted by key,
// and the handler context's deadline.
func printMessage(n int32, ctx context.Context, m mesh.Message) {
	keys := make([]string, 0, len(m.Metadata))
	for k := range m.Metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "call %d: payload %q\n", n, m.Payload)
	for _, k := range keys {
		fmt.Fprintf(&b, "    %s: %s\n", k, m.Metadata[k])
	}
	if d, ok := ctx.Deadline(); ok {
		fmt.Fprintf(&b, "    context deadline: %s (in %s)", d.Format(time.RFC3339Nano), time.Until(d).Round(time.Millisecond))
	} else {
		b.WriteString("    context deadline: none")
	}
	log.Print(b.String())
}

// printReceiver prints the resolved consumer group and, for a JetStream
// subscriber, the settings the server holds for its consumer.
func printReceiver(rt *nats.Runtime, t mesh.Target) {
	rc, ok := rt.Receiver(t)
	if !ok {
		return
	}
	log.Printf("consumer group: %s", rc.ConsumerGroup())
	cons := rc.Consumer()
	if cons == nil {
		log.Print("core NATS: no JetStream consumer")
		return
	}
	info, err := cons.Info(context.Background())
	if err != nil {
		log.Printf("consumer info: %v", err)
		return
	}
	c := info.Config
	log.Printf("JetStream consumer %q on stream %q: ack_policy=%s ack_wait=%s max_deliver=%d backoff=%v max_ack_pending=%d deliver_policy=%s inactive_threshold=%s",
		info.Name, info.Stream, c.AckPolicy, c.AckWait, c.MaxDeliver, c.BackOff, c.MaxAckPending, c.DeliverPolicy, c.InactiveThreshold)
}
