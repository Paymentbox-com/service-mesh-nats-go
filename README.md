# service-mesh-nats-go

A Go implementation of the
[Service Mesh API Specification](https://github.com/Paymentbox-com/service-mesh-api)
over NATS. Module path `github.com/Paymentbox-com/service-mesh-nats-go`. The
specification is the authority for everything this package does. The Go
contract it implements is
[service-mesh-go](https://github.com/Paymentbox-com/service-mesh-go), and the
Ruby implementation is
[service-mesh-nats-ruby](https://github.com/Paymentbox-com/service-mesh-nats-ruby).
The [gRPC Service Mesh API](https://github.com/Paymentbox-com/grpc-service-mesh-api)
is the protocol layer that generates code served over this transport from
protobuf definitions, through its Go library
[grpc-service-mesh-go](https://github.com/Paymentbox-com/grpc-service-mesh-go).

One package, `nats`, a runtime for NATS on `nats.go`. It exports `New`,
`NewClient`, its configuration keys, and two `Option`s. The contract types it
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
    nats.URLKey:         "nats://127.0.0.1:4222",
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
state, and `Request` and `Publish` on it after `Stop` return `nats.ErrClosed`.
A process that only calls uses `nats.NewClient(cfg, sm)` on its own and
closes it when done. `client.ServiceMap()` and `rt.ServiceMap()` return the
map the client was built with; the transport does not validate targets
against it. `examples/echo` is a single-process version of the above.
`examples/server` and `examples/client` split it across two processes;
`E2E.md` walks through them.

## Examples

Each snippet below runs as written against a local `nats-server`. The
`echo`, `created`, and `sm` values are the ones declared under Usage.

### A server process

Serves one endpoint and one subscriber until SIGINT or SIGTERM, then drains
for up to ten seconds.

```go
func main() {
    cfg := mesh.Config{nats.URLKey: os.Getenv("NATS_URL"), mesh.DeploymentGroupKey: "demo"}

    client, err := nats.NewClient(cfg, sm)
    if err != nil {
        log.Fatal(err)
    }
    rt, err := nats.New(client, cfg,
        []mesh.Endpoint{{Target: echo, Handler: func(ctx context.Context, m mesh.Message) (mesh.Message, error) {
            return mesh.Message{Payload: m.Payload}, nil
        }}},
        []mesh.Subscriber{{Target: created, Handler: func(ctx context.Context, m mesh.Message) error {
            log.Printf("created: %s", m.Payload)
            return nil
        }}},
    )
    if err != nil {
        log.Fatal(err)
    }
    if err := rt.Start(context.Background()); err != nil {
        log.Fatal(err)
    }

    stop := make(chan os.Signal, 1)
    signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
    <-stop

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // drain budget
    defer cancel()
    if err := rt.Stop(ctx); err != nil { // closes client
        log.Printf("stop: %v", err) // handlers were abandoned
    }
}
```

### A call-only client process

Makes one request with metadata and a per-call timeout, sorts the outcomes,
then publishes an event.

```go
c, err := nats.NewClient(mesh.Config{nats.URLKey: os.Getenv("NATS_URL")}, sm)
if err != nil {
    log.Fatal(err)
}
defer func() { _ = c.Close() }()

ctx := context.Background()
reply, err := c.Request(ctx, mesh.Message{
    Target:   echo,
    Metadata: map[string]string{"Request-Id": "1"},
    Payload:  []byte("hello"),
}, map[string]string{nats.RequestTimeoutKey: "2s"})

var he *nats.HandlerError
switch {
case err == nil:
    fmt.Printf("%s %v\n", reply.Payload, reply.Metadata)
case errors.Is(err, mesh.ErrKindMismatch): // a topic target given to Request
case errors.Is(err, nats.ErrNoResponders): // nothing serves demo.echo
case errors.Is(err, nats.ErrTimeout):      // no reply within request_timeout
case errors.Is(err, nats.ErrClosed):       // c.Close has run
case errors.As(err, &he):                  // the handler failed: he.Text
default:                                   // any other nats.go error, unchanged
}

if err := c.Publish(ctx, mesh.Message{Target: created, Payload: []byte("order 42")}, nil); err != nil {
    log.Fatal(err)
}
```

### Consumer groups

Two deployments on one topic each handle every event once. A subscriber
with `consumer_group` set to `none` handles every event on every instance.
Each runtime is built from its own client, since a client is one connection.

```go
onCreated := func(ctx context.Context, m mesh.Message) error {
    log.Printf("created: %s", m.Payload)
    return nil
}

// serve builds a client and a runtime for one deployment group.
serve := func(group string, sub mesh.Subscriber) *nats.Runtime {
    cfg := mesh.Config{nats.URLKey: url, mesh.DeploymentGroupKey: group}
    client, err := nats.NewClient(cfg, sm)
    if err != nil {
        log.Fatal(err)
    }
    rt, err := nats.New(client, cfg, nil, []mesh.Subscriber{sub})
    if err != nil {
        log.Fatal(err)
    }
    return rt
}

// billing and audit each run this subscriber under their own
// deployment_group, so every event is handled once per deployment.
sub := mesh.Subscriber{Target: created, Handler: onCreated}
billing := serve("billing", sub)
audit := serve("audit", sub)

// Every instance of a deployment handles every event: no group at all.
broadcast := mesh.Subscriber{
    Target:   created,
    Metadata: map[string]string{mesh.ConsumerGroupKey: mesh.ConsumerGroupNone},
    Handler:  onCreated,
}
cache := serve("cache", broadcast)

for _, rt := range []*nats.Runtime{billing, audit, cache} {
    if err := rt.Start(ctx); err != nil {
        log.Fatal(err)
    }
}
// One publish to created now produces three "created" lines: billing, audit, cache.
```

## What the NATS runtime decides

The specification leaves these to each transport. Full detail is in the
`nats` package documentation.

**Targets.** Segments join with `.` into a subject. A segment must be
non-empty and free of `.`, `*`, `>`, whitespace, and non-printable
characters. Targets are literal; no wildcards.

**Configuration.** All values are strings. `NewClient` reads the connection
keys and `New` reads `deployment_group`, which is required, and
`concurrency`. Each constructor ignores the other's keys, so one `Config`
can be given to both.

| key                | read by     | default            | meaning                                            |
|--------------------|-------------|--------------------|----------------------------------------------------|
| `url`              | `NewClient` | `nats.DefaultURL`  | server URL or comma-separated list                 |
| `name`             | `NewClient` | none               | connection name reported to the server             |
| `connect_timeout`  | `NewClient` | `5s`               | bound on the initial connection, Go duration       |
| `request_timeout`  | `NewClient` | `30s`              | bound on `Request` when ctx has no deadline, Go duration; also accepted as a per-call option |
| `deployment_group` | `New`       | required           | the queue group bindings join                      |
| `concurrency`      | `New`       | CPU count          | max handlers running at once                       |

A value that does not parse yields `ErrBadConfig`. Extra nats.go connection
options are passed to `NewClient` as `nats.WithNATSOptions`, and a logger to
`New` as `nats.WithLogger`. The package re-exports `ErrNoResponders` and
`ErrTimeout` from nats.go so callers need not import it.

**Metadata.** Message metadata rides as NATS headers, one value per key. The
runtime reads no message keys and writes `HandlerErrorHeader` on a failed
reply. Binding metadata is read at construction. `consumer_group` is taken
from the `Endpoint` or `Subscriber` first, then from its `Target`.
`deployment_group` on a client-side `Target` is ignored.

**Delivery.** A consumer group is a NATS queue group. Every endpoint and
subscriber joins the deployment group unless `consumer_group` overrides it.
`none` gives a plain subscription, so every instance handles every message,
and for an endpoint every instance replies. Two bindings on one subject are
two NATS subscriptions, with whatever delivery NATS gives them.

**Handler failure.** An endpoint handler that returns an error or panics
produces an empty reply carrying the error text in `HandlerErrorHeader`; the
requester receives `*nats.HandlerError`. A failing subscriber handler is
logged.

**Timeouts.** `Request` with no context deadline is bounded by the request
timeout and reports expiry as `nats.ErrTimeout`. A context deadline or
cancellation is reported as the context's own error.

**Concurrency.** At most `concurrency` handlers run at once across all
bindings. Beyond that, deliveries wait in nats.go's pending buffer.

**Lifecycle.** A `Client` owns a NATS connection. `NewClient` returns a
connected one, and its `Close` closes the connection; `Close` is idempotent.
A `Runtime` is built from a `Client` the application constructed, and that
client is the runtime's connection. `New` takes a `*nats.Client`, not a
`mesh.Client`, because the runtime subscribes through the NATS connection the
client owns. `Runtime.Client()` returns it in every state, and
`Runtime.ServiceMap()` is the client's. `Start` subscribes every binding on
the client's connection and flushes; on a closed client it returns
`ErrClosed`, and a subscribe or flush failure leaves the runtime and the
client as they were. `Stop` unsubscribes, waits for in-flight handlers until
its context is done, cancels the handlers' context, flushes, and closes the
client; it returns the context's error when handlers were abandoned. The
specification's drain in seconds is the context's deadline. A runtime does
not restart. Closing the client directly ends the runtime's connection;
`Stop` afterwards returns the drain result.

**Errors.** The package defines `ErrBadConfig`, `ErrAlreadyStarted`,
`ErrStopped`, `ErrClosed` (a `Request` or `Publish` after `Close`, which for
a runtime's client is after `Stop`, and a `Start` on a runtime whose client
is closed), and `*HandlerError`. The three contract errors come from `mesh`.

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
