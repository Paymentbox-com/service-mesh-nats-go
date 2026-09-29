# service-mesh-nats-go

A Go implementation of the
[Service Mesh API Specification](https://github.com/Paymentbox-com/service-mesh-api)
over NATS. Module path `github.com/Paymentbox-com/service-mesh-nats-go`. The
specification is the authority for everything this package does. The Go
contract it implements is
[service-mesh-go](https://github.com/Paymentbox-com/service-mesh-go).

One package, `nats`, a runtime for NATS on `nats.go`. It exports `New`,
`NewClient`, its configuration keys, and `WithLogger`. The contract types it
implements, `mesh.Target`, `mesh.Message`, `mesh.Endpoint`, `mesh.Subscriber`,
`mesh.Client`, `mesh.Runtime`, and the rest, come from
`github.com/Paymentbox-com/service-mesh-go/mesh`.

## Usage

```go
import (
    "github.com/Paymentbox-com/service-mesh-go/mesh"
    "github.com/Paymentbox-com/service-mesh-nats-go/nats"
)

var (
    echo    = mesh.Target{Segments: []string{"demo", "echo"}, Kind: mesh.KindRoute}
    created = mesh.Target{Segments: []string{"demo", "created"}, Kind: mesh.KindTopic}
    sm      = mesh.ServiceMap{Targets: []mesh.Target{echo, created}}
)

cfg := mesh.Config{
    nats.URLKey:             "nats://127.0.0.1:4222",
    mesh.DeploymentGroupKey: "demo",
}

client, err := nats.NewClient(cfg, sm) // connects; reads url and the other connection keys
if err != nil { /* nats.ErrBadConfig, or the nats.go connection error */ }

rt, err := nats.New(client, cfg, // reads deployment_group and concurrency
    []mesh.Endpoint{{Target: echo, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
        return mesh.Message{Payload: m.Payload}, nil
    }}},
    []mesh.Subscriber{{Target: created, Handler: func(ctx context.Context, m mesh.Message) error {
        log.Printf("created: %s", m.Payload)
        return nil
    }}},
)
if err != nil { /* mesh.ErrNoDeploymentGroup, mesh.ErrKindMismatch, mesh.ErrInvalidTarget, nats.ErrBadConfig */ }

if err := rt.Start(ctx); err != nil { /* subscribe failure, or nats.ErrClosed when client is closed */ }
defer rt.Stop(ctx) // closes client

reply, err := client.Request(ctx, mesh.Message{Target: echo, Payload: []byte("hi")}, nil)
err = client.Publish(ctx, mesh.Message{Target: created, Payload: []byte("order 42")}, nil)
```

The client is the runtime's connection. `rt.Client()` returns it in whatever state
it is in, and `Request` and `Publish` on it after `Stop` return `nats.ErrClosed`.
A process that only makes requests or publishes uses `nats.NewClient(cfg, sm)` on
its own and closes it when done. `client.ServiceMap()` and `rt.ServiceMap()` return the
map the client was built with. `examples/echo` is a single-process version of the above.
`examples/server` and `examples/client` run the two sides as separate processes, and the [end-to-end walkthrough](docs/e2e.md)
covers testing each side.

## Documentation

- [Examples](docs/examples.md): a server process, a call-only client process, and consumer groups
- [What the NATS Runtime Decides](docs/runtime-behavior.md): subjects, configuration, metadata, delivery, handler failure, timeouts, concurrency, lifecycle, and errors
- [End-to-End Walkthrough](docs/e2e.md): the example server and client run against a local broker
- [Development](docs/development.md): tools, recipes, and tests
