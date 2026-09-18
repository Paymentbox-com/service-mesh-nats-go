// Command client sends one message from its own process.
//
//	go run ./examples/client request hello
//	go run ./examples/client publish "order 42"
//
// Use the payload "fail" to make the server's handler return an error, and
// "slow" to make it take five seconds. NATS_URL overrides the server
// address; TIMEOUT sets the request timeout, for example TIMEOUT=2s.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Paymentbox-com/service-mesh-go/mesh"
	"github.com/Paymentbox-com/service-mesh-nats-go/nats"
)

var (
	echoTarget    = mesh.Target{Segments: []string{"demo", "echo"}, Kind: mesh.KindRoute}
	createdTarget = mesh.Target{Segments: []string{"demo", "created"}, Kind: mesh.KindTopic}
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: client request|publish <payload>")
		os.Exit(2)
	}
	verb, payload := os.Args[1], os.Args[2]

	cfg := mesh.Config{nats.URLKey: os.Getenv("NATS_URL"), nats.NameKey: "client"}
	opts := map[string]string{}
	if t := os.Getenv("TIMEOUT"); t != "" {
		opts[nats.RequestTimeoutKey] = t
	}

	c, err := nats.NewClient(cfg)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	msg := mesh.Message{Metadata: map[string]string{"Sent-At": time.Now().Format(time.RFC3339Nano)}, Payload: []byte(payload)}

	switch verb {
	case "request":
		msg.Target = echoTarget
		start := time.Now()
		reply, err := c.Request(ctx, msg, opts)
		if err != nil {
			report(err)
			os.Exit(1)
		}
		fmt.Printf("reply after %s: %q metadata %v\n", time.Since(start).Round(time.Millisecond), reply.Payload, reply.Metadata)
	case "publish":
		msg.Target = createdTarget
		if err := c.Publish(ctx, msg, opts); err != nil {
			report(err)
			os.Exit(1)
		}
		fmt.Println("published")
	default:
		fmt.Fprintln(os.Stderr, "usage: client request|publish <payload>")
		os.Exit(2)
	}
}

// report names the kind of failure so each step in E2E.md has a
// recognisable outcome.
func report(err error) {
	var he *nats.HandlerError
	switch {
	case errors.As(err, &he):
		fmt.Printf("handler failed on the server: %s\n", he.Text)
	case errors.Is(err, nats.ErrNoResponders):
		fmt.Println("no server is serving demo.echo")
	case errors.Is(err, nats.ErrTimeout):
		fmt.Println("request timed out")
	case errors.Is(err, mesh.ErrKindMismatch), errors.Is(err, mesh.ErrInvalidTarget), errors.Is(err, nats.ErrBadConfig):
		fmt.Printf("contract misuse: %v\n", err)
	default:
		fmt.Printf("transport error: %v\n", err)
	}
}
