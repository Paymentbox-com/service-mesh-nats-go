// Command echo serves an echo endpoint and an event subscriber over NATS,
// then calls both from a separate client and exits.
//
// Run a local server first:
//
//	nats-server
//	go run ./examples/echo
//
// NATS_URL overrides the server address.
package main

import (
	"context"
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
	serviceMap    = mesh.ServiceMap{Targets: []mesh.Target{echoTarget, createdTarget}}
)

func main() {
	cfg := mesh.Config{nats.URLKey: os.Getenv("NATS_URL"), mesh.DeploymentGroupKey: "demo"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	received := make(chan mesh.Message, 1)

	rt, err := nats.New(cfg, serviceMap,
		[]mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
			return mesh.Message{
				Metadata: map[string]string{"Echoed-By": "demo"},
				Payload:  append([]byte("echo: "), m.Payload...),
			}, nil
		}}},
		[]mesh.Subscriber{{Target: createdTarget, Handler: func(ctx context.Context, m mesh.Message) error {
			received <- m
			return nil
		}}},
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := rt.Start(ctx); err != nil {
		log.Fatalf("start: %v", err)
	}
	defer func() {
		if err := rt.Stop(ctx); err != nil {
			log.Printf("stop: %v", err)
		}
	}()

	client, err := nats.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	reply, err := client.Request(ctx, mesh.Message{
		Target:   echoTarget,
		Metadata: map[string]string{"Request-Id": "1"},
		Payload:  []byte("hello"),
	}, nil)
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	fmt.Printf("reply: %s (metadata %v)\n", reply.Payload, reply.Metadata)

	if err := client.Publish(ctx, mesh.Message{Target: createdTarget, Payload: []byte("order 42")}, nil); err != nil {
		log.Fatalf("publish: %v", err)
	}
	select {
	case m := <-received:
		fmt.Printf("subscriber got: %s\n", m.Payload)
	case <-ctx.Done():
		log.Fatal("subscriber did not receive the event")
	}

	// The contract catches misuse before anything reaches the transport.
	if err := client.Publish(ctx, mesh.Message{Target: echoTarget}, nil); err != nil {
		fmt.Printf("publish to a route target: %v\n", err)
	}
}
