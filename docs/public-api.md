# Public API

Every exported name in package `nats`.

| Name | Role |
|---|---|
| `NewClient(cfg, serviceMap)` | Opens one NATS connection and returns a connected `*Client`, as described under [The Client](transport-specific-implementation.md#the-client). |
| `Client` | Implements `mesh.Client`. Its methods are `Request(ctx, msg, opts)`, `Publish(ctx, msg, opts)`, `Close()`, and `ServiceMap()`. |
| `New(client, cfg, endpoints, subscribers, opts...)` | Builds a `*Runtime` that serves the endpoints and subscribers on the client's connection, as described under [The Runtime and Its Client](transport-specific-implementation.md#the-runtime-and-its-client). |
| `Runtime` | Implements `mesh.Runtime`. Its methods are `Start(ctx)`, `Stop(ctx)`, `Running()`, `Client()`, and `ServiceMap()`. |
| `Option`, `WithLogger(logger)` | Options for `New`. `WithLogger` sets the `*slog.Logger` the runtime logs to. |
| `URLKey`, `NameKey`, `ConnectTimeoutKey`, `RequestTimeoutKey`, `ConcurrencyKey` | The configuration keys, described under [Configuration](transport-specific-implementation.md#configuration). `RequestTimeoutKey` is also a per-call option. |
| `HandlerErrorHeader` | `"Mesh-Handler-Error"`, the reply header the runtime sets when an endpoint handler fails. |
| `ErrBadConfig`, `ErrClosed`, `ErrAlreadyStarted`, `ErrStopped`, `HandlerError` | The errors this package defines, listed under [Errors](transport-specific-implementation.md#errors). |
| `ErrNoResponders`, `ErrTimeout` | The nats.go errors callers test for most often, re-exported so callers need not import nats.go. |
