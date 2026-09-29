# Examples

These examples show the three shapes a process using this transport usually
takes: one that serves, one that only calls, and several deployments sharing a
topic. Each snippet runs against a local `nats-server`, and uses the `echo`,
`created`, and `sm` values declared under [Usage](../README.md#usage).
`NATS_URL` points a snippet at a different server, and an empty value takes the
default.

## A Server Process

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

## A Call-Only Client Process

Makes one request with metadata and a per-call timeout, handles each outcome,
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

## Consumer Groups

Two deployments on one topic each handle every event once. A subscriber
with `consumer_group` set to `none` handles every event on every instance.
Each runtime is built from its own client, since a client is one connection.

```go
url := os.Getenv("NATS_URL")
ctx := context.Background()
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

// With no consumer group, every instance of a deployment handles every event.
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
