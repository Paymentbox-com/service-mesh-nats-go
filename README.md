# service-mesh-nats-go

A Go implementation of the Service Mesh API Specification over NATS. Module
path `github.com/Paymentbox-com/service-mesh-nats-go`.

Two packages:

- `mesh` is the contract. `Target`, `ServiceMap`, `Message`, `Endpoint`,
  `Subscriber`, the handler types, the `Client` and `Runtime` interfaces,
  `Config`, the `deployment_group` and `consumer_group` keys, and the three
  errors. No dependencies.
- `nats` is a runtime for NATS on `nats.go`. It exports `New`,
  `NewClient`, its configuration keys, and two `Option`s.

## Usage

```go
import (
    "github.com/Paymentbox-com/service-mesh-nats-go/mesh"
    "github.com/Paymentbox-com/service-mesh-nats-go/nats"
)

var (
    echo    = mesh.Target{Segments: []string{"demo", "echo"}, Kind: mesh.KindRoute}
    created = mesh.Target{Segments: []string{"demo", "created"}, Kind: mesh.KindTopic}
    sm      = mesh.ServiceMap{Targets: []mesh.Target{echo, created}}
)

cfg := mesh.Config{
    nats.URLKey:         "nats://127.0.0.1:4222",
    mesh.DeploymentGroupKey: "demo",
}

rt, err := nats.New(cfg, sm,
    []mesh.Endpoint{{Target: echo, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
        return mesh.Message{Payload: m.Payload}, nil
    }}},
    []mesh.Subscriber{{Target: created, Handler: func(ctx context.Context, m mesh.Message) error {
        log.Printf("created: %s", m.Payload)
        return nil
    }}},
)
if err != nil { /* mesh.ErrNoDeploymentGroup, mesh.ErrKindMismatch, mesh.ErrInvalidTarget, nats.ErrBadConfig, nats.ErrDuplicateTarget */ }

if err := rt.Start(ctx); err != nil { /* connect or subscribe failure */ }
defer rt.Stop(ctx)

c := rt.Client()
reply, err := c.Request(ctx, mesh.Message{Target: echo, Payload: []byte("hi")}, nil)
err = c.Publish(ctx, mesh.Message{Target: created, Payload: []byte("order 42")}, nil)
```

A process that only calls uses `nats.NewClient(cfg)` and closes it when
done. `examples/echo` is a single-process version of the above.
`examples/server` and `examples/client` split it across two processes;
`E2E.md` walks through them.

## What the NATS runtime decides

The specification leaves these to each transport. Full detail is in the
`nats` package documentation.

**Targets.** Segments join with `.` into a subject. A segment must be
non-empty and free of `.`, `*`, `>`, whitespace, and non-printable
characters. Targets are literal; no wildcards.

**Configuration.** All values are strings. Beyond `deployment_group` the
keys are:

| key               | default            | meaning                                            |
|-------------------|--------------------|----------------------------------------------------|
| `url`             | `nats.DefaultURL`  | server URL or comma-separated list                 |
| `name`            | none               | connection name reported to the server             |
| `connect_timeout` | `5s`               | bound on the initial connection, Go duration       |
| `request_timeout` | `30s`              | bound on `Request` when ctx has no deadline, Go duration; also accepted as a per-call option |
| `concurrency`     | CPU count          | max handlers running at once                       |

A value that does not parse yields `ErrBadConfig`. A logger and extra
nats.go connection options are passed as `nats.WithLogger` and
`nats.WithNATSOptions`. The package re-exports `ErrNoResponders` and
`ErrTimeout` from nats.go so callers need not import it.

**Metadata.** Message metadata rides as NATS headers, one value per key. The
runtime reads no message keys and writes `HandlerErrorHeader` on a failed
reply. Binding metadata is read at construction. `consumer_group` is taken
from the `Endpoint` or `Subscriber` first, then from its `Target`.
`deployment_group` on a client-side `Target` is ignored.

**Delivery.** A consumer group is a NATS queue group. Every endpoint and
subscriber joins the deployment group unless `consumer_group` overrides it.
`none` gives a plain subscription, so every instance handles every message,
and for an endpoint every instance replies.

**Handler failure.** An endpoint handler that returns an error or panics
produces an empty reply carrying the error text in `HandlerErrorHeader`; the
requester receives `*nats.HandlerError`. A failing subscriber handler is
logged.

**Timeouts.** `Request` with no context deadline is bounded by the request
timeout and reports expiry as `nats.ErrTimeout`. A context deadline or
cancellation is reported as the context's own error.

**Concurrency.** At most `concurrency` handlers run at once across all
bindings. Beyond that, deliveries wait in nats.go's pending buffer.

**Lifecycle.** `Start` connects, subscribes, and flushes. `Stop` unsubscribes,
waits for in-flight handlers until its context is done, cancels the handlers'
context, flushes, and closes; it returns the context's error when handlers
were abandoned. The specification's drain in seconds is the context's
deadline. A runtime does not restart. `Runtime.Client()` shares the
connection; its `Close` is a no-op and it returns `ErrNotRunning` outside the
running window.

**Transport errors.** nats.go errors come back unchanged, most often
`nats.ErrNoResponders` and `nats.ErrTimeout`.

## Development

Tool versions are pinned in `mise.toml` and installed with `mise install`.
`just` lists the recipes. The ones used day to day:

| recipe          | what it does                                            |
|-----------------|---------------------------------------------------------|
| `just build`    | compile everything                                      |
| `just test`     | run the suite with the race detector                    |
| `just examples` | build the example programs                              |
| `just check`    | format check, vet, test, vulnerability scan, lint; what CI runs |

Integration tests start an embedded `nats-server` on a random loopback port,
so they need permission to bind a local TCP socket.
