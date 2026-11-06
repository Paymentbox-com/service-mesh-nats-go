// Command streams creates, inspects, and deletes the JetStream streams and
// consumers the local behavior runbook in docs/runbooks/local-behavior.md
// uses, so the runbook needs no tool beyond Go and nats-server.
//
//	go run ./docs/runbooks/cmd/streams add DEMO_CREATED demo.created
//	go run ./docs/runbooks/cmd/streams info DEMO_CREATED
//	go run ./docs/runbooks/cmd/streams consumers DEMO_CREATED
//	go run ./docs/runbooks/cmd/streams add-consumer DEMO_CREATED audit demo.created
//	go run ./docs/runbooks/cmd/streams rm DEMO_CREATED
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const usage = `usage:
  streams add NAME SUBJECT [limits|interest|workqueue]
  streams info NAME
  streams consumers NAME
  streams add-consumer STREAM NAME SUBJECT
  streams rm NAME`

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = natsio.DefaultURL
	}
	nc, err := natsio.Connect(url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := os.Args[2:]
	switch os.Args[1] {
	case "add":
		cfg := jetstream.StreamConfig{Name: args[0], Subjects: []string{args[1]}}
		if len(args) > 2 {
			switch args[2] {
			case "interest":
				cfg.Retention = jetstream.InterestPolicy
			case "workqueue":
				cfg.Retention = jetstream.WorkQueuePolicy
			}
		}
		s, err := js.CreateStream(ctx, cfg)
		if err != nil {
			log.Fatalf("add: %v", err)
		}
		fmt.Printf("created stream %q for %v with %s retention\n", s.CachedInfo().Config.Name, cfg.Subjects, s.CachedInfo().Config.Retention)
	case "info":
		s, err := js.Stream(ctx, args[0])
		if err != nil {
			log.Fatalf("info: %v", err)
		}
		info, err := s.Info(ctx)
		if err != nil {
			log.Fatalf("info: %v", err)
		}
		fmt.Printf("stream %q: subjects %v, %d messages, sequences %d to %d, %d consumers\n",
			info.Config.Name, info.Config.Subjects, info.State.Msgs, info.State.FirstSeq, info.State.LastSeq, info.State.Consumers)
	case "consumers":
		s, err := js.Stream(ctx, args[0])
		if err != nil {
			log.Fatalf("consumers: %v", err)
		}
		list := s.ListConsumers(ctx)
		for info := range list.Info() {
			c := info.Config
			fmt.Printf("consumer %q: pending %d, ack pending %d, redelivered %d, ack_policy=%s ack_wait=%s max_deliver=%d inactive_threshold=%s\n",
				info.Name, info.NumPending, info.NumAckPending, info.NumRedelivered, c.AckPolicy, c.AckWait, c.MaxDeliver, c.InactiveThreshold)
		}
		if err := list.Err(); err != nil {
			log.Fatalf("consumers: %v", err)
		}
	case "add-consumer":
		c, err := js.CreateConsumer(ctx, args[0], jetstream.ConsumerConfig{
			Durable:       args[1],
			FilterSubject: args[2],
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       time.Minute,
		})
		if err != nil {
			log.Fatalf("add-consumer: %v", err)
		}
		fmt.Printf("created consumer %q on stream %q with ack_wait=1m\n", c.CachedInfo().Name, args[0])
	case "rm":
		if err := js.DeleteStream(ctx, args[0]); err != nil {
			log.Fatalf("rm: %v", err)
		}
		fmt.Printf("deleted stream %q\n", args[0])
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
}
