# service-mesh-nats-go

`nats` is the Go implementation of the
[Service Mesh API Specification](https://github.com/Paymentbox-com/service-mesh-api)
over NATS. It implements the Go contract in
[service-mesh-go](https://github.com/Paymentbox-com/service-mesh-go) on top of
the `nats.go` client.

The `nats` package provides:
* `Client`, built with `NewClient`, which implements `mesh.Client` over one NATS connection
* `Runtime`, built with `New`, which implements `mesh.Runtime` on a `Client`'s connection
* the configuration keys it reads, and `WithLogger`
* the errors it defines

The contract types it works with, such as `mesh.Target`, `mesh.Message`,
`mesh.Endpoint`, and `mesh.Subscriber`, come from
`github.com/Paymentbox-com/service-mesh-go/mesh`.

## Install

The module is `github.com/Paymentbox-com/service-mesh-nats-go`, and its one
package, `nats`, is imported as
`github.com/Paymentbox-com/service-mesh-nats-go/nats`.

```sh
go get github.com/Paymentbox-com/service-mesh-nats-go
```

Requires Go 1.26 or newer and a reachable NATS server. The module depends on
`github.com/Paymentbox-com/service-mesh-go/mesh` and `github.com/nats-io/nats.go`.

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

The client is the runtime's connection. `rt.Client()` returns it in every
state, and `rt.Stop` closes it. A process that only requests and publishes
builds a client itself and calls `Close` when done. `rt.ServiceMap()` and
`client.ServiceMap()` return the map the client was built with.

`examples/echo` is a single-process version of the above. `examples/server`
and `examples/client` run the two sides as separate processes, and the
[End-to-End Walkthrough](docs/e2e.md) runs them against a local server.

## Documentation

- [Examples](docs/examples.md): a server process, a call-only client process, and consumer groups
- [Public API](docs/public-api.md): every exported name in `nats`
- [Transport Specific Implementation](docs/transport-specific-implementation.md): subjects, configuration, metadata, delivery, handler failure, timeouts, concurrency, lifecycle, and errors
- [End-to-End Walkthrough](docs/e2e.md): the example server and client run against a local NATS server
- [Development](docs/development.md): the recipes and the tests
