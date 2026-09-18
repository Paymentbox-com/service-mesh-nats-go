// Command server serves demo.echo and subscribes to demo.created until it
// receives SIGINT or SIGTERM, then drains and exits.
//
//	go run ./examples/server
//
// NATS_URL overrides the server address. DEPLOYMENT sets the deployment
// group (default "demo"); instances with the same value share each request
// and event. INSTANCE labels this process in its log lines and reply
// metadata.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	instance := os.Getenv("INSTANCE")
	if instance == "" {
		instance = "server"
	}
	deployment := os.Getenv("DEPLOYMENT")
	if deployment == "" {
		deployment = "demo"
	}
	log.SetPrefix("[" + instance + "] ")

	cfg := mesh.Config{
		nats.URLKey:             os.Getenv("NATS_URL"),
		nats.NameKey:            instance,
		mesh.DeploymentGroupKey: deployment,
	}

	rt, err := nats.New(cfg, serviceMap,
		[]mesh.Endpoint{{Target: echoTarget, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
			log.Printf("echo request %q metadata %v", m.Payload, m.Metadata)
			if string(m.Payload) == "fail" {
				return mesh.Message{}, os.ErrInvalid
			}
			if string(m.Payload) == "slow" {
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return mesh.Message{}, ctx.Err()
				}
			}
			return mesh.Message{
				Metadata: map[string]string{"Echoed-By": instance},
				Payload:  append([]byte("echo: "), m.Payload...),
			}, nil
		}}},
		[]mesh.Subscriber{{Target: createdTarget, Handler: func(ctx context.Context, m mesh.Message) error {
			log.Printf("created event %q metadata %v", m.Payload, m.Metadata)
			return nil
		}}},
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := rt.Start(context.Background()); err != nil {
		log.Fatalf("start: %v", err)
	}
	log.Print("serving demo.echo and demo.created; Ctrl-C to stop")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Print("stopping, draining up to 10s")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rt.Stop(ctx); err != nil {
		log.Printf("stop: %v", err)
	}
	log.Print("stopped")
}
