# Public API

Every exported name in package `nats`.

| Name | Role |
|---|---|
| `NewClient(cfg, serviceMap)` | Opens one NATS connection and returns a connected `*Client`, as described under [The Client](transport-specific-implementation.md#the-client). |
| `Client` | Implements `mesh.Client`. Its methods are `Request(ctx, msg, opts)`, `Publish(ctx, msg, opts)`, `Close()`, and `ServiceMap()`. It also has `Conn()`, `JetStream()`, and `Stream(ctx, target)`, described under [Inspecting Streams and Consumers](jetstream.md#inspecting-streams-and-consumers). |
| `New(client, cfg, endpoints, subscribers, opts...)` | Builds a `*Runtime` that serves the endpoints and subscribers on the client's connection, as described under [The Runtime and Its Client](transport-specific-implementation.md#the-runtime-and-its-client). |
| `Runtime` | Implements `mesh.Runtime`. Its methods are `Start(ctx)`, `Stop(ctx)`, `Running()`, `Client()`, and `ServiceMap()`. It also has `Subscriber(target)`. |
| `Subscriber` | A subscriber the runtime is running. Its methods are `Target()`, `ConsumerGroup()`, `Stream(ctx)`, and `Consumer()`, described under [Inspecting Streams and Consumers](jetstream.md#inspecting-streams-and-consumers). |
| `Option`, `WithLogger(logger)` | Options for `New`. `WithLogger` sets the `*slog.Logger` the runtime logs to. |
| `URLKey`, `NameKey`, `ConnectTimeoutKey`, `RequestTimeoutKey`, `ConcurrencyKey`, `MaxDeliverKey` | The configuration keys, described under [Configuration](transport-specific-implementation.md#configuration). `RequestTimeoutKey` is also a setting of `Request` and `Publish`, and `MaxDeliverKey` is also a subscriber setting, as described under [Settings](transport-specific-implementation.md#settings). |
| `StreamKey`, `CreateConsumerKey`, `AckPolicyKey`, `AckWaitKey`, `BackoffKey`, `NakDelayKey`, `MaxAckPendingKey`, `DeliverPolicyKey`, `ConsumerInactiveThresholdKey` | The JetStream options, described under [Options](jetstream.md#options). |
| `StreamMetadataKey`, `ConsumerMetadataKey`, `StreamSequenceMetadataKey`, `ConsumerSequenceMetadataKey`, `NumPendingMetadataKey`, `TimestampMetadataKey` | The `Mesh-Nats-` keys on a JetStream delivery, described under [Delivery Metadata](jetstream.md#delivery-metadata). |
| `ErrBadConfig`, `ErrClosed`, `ErrAlreadyStarted`, `ErrStopped`, `ErrStreamCoversRoute`, `HandlerError` | The errors this package defines, listed under [Errors](transport-specific-implementation.md#errors). |
| `ErrNoResponders`, `ErrTimeout` | The nats.go errors callers test for most often, re-exported so callers need not import nats.go. |
