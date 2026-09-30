# Transport Specific Implementation

The [Service Mesh API
Specification](https://github.com/Paymentbox-com/service-mesh-api) leaves some
behavior to each transport, such as how a `Target` becomes an address, what
configuration it reads, and how delivery and failures work. This page describes
what this transport does in each of those places. The `nats` package
documentation has the same detail on each exported name.

## Targets

A target's segments join with `.` into a NATS subject. A segment must be
non-empty and free of `.`, `*`, `>`, whitespace, and non-printable characters.
Targets are literal and hold no wildcards.

## Configuration

All values are strings. `NewClient` reads the connection keys, and `New` reads
`deployment_group`, which is required, and `concurrency`. Each constructor
ignores the other's keys, so one `Config` can be given to both.

| Key | Read by | Default | Meaning |
|---|---|---|---|
| `url` | `NewClient` | `nats://127.0.0.1:4222` | The server URL, or a comma-separated list of them. An empty value takes the default. |
| `name` | `NewClient` | none | The connection name reported to the server. |
| `connect_timeout` | `NewClient` | `5s` | The bound on the initial connection, as a Go duration. |
| `request_timeout` | `NewClient` | `30s` | The bound on `Request` when its context has no deadline, as a Go duration. It is also accepted as a per-call option. |
| `deployment_group` | `New` | required | The queue group that endpoints and subscribers join. |
| `concurrency` | `New` | CPU count | The most handlers that run at once. |

A value that does not parse returns `ErrBadConfig`. A logger is passed to `New`
with `nats.WithLogger`, and defaults to `slog.Default()`.

## Metadata

Message metadata travels as NATS headers, one value per key. The runtime reads
no message keys, and writes `HandlerErrorHeader` (`Mesh-Handler-Error`) on a
failed reply.

Endpoint and subscriber metadata is read when `New` runs, and its
`consumer_group` selects the queue group. A `Target` carries no
`consumer_group` or `deployment_group`, so a target's metadata is not read for
either.

## Delivery

A consumer group is a NATS queue group. Every endpoint and subscriber joins the
runtime's `deployment_group` unless its own `consumer_group` overrides it. An
empty `consumer_group` counts as unset.

`none` gives a plain subscription, so every instance handles every message, and
for an endpoint every instance replies.

Two endpoints or subscribers on one subject are two NATS subscriptions, with
whatever delivery NATS gives them.

## Handler Failure

An endpoint handler that returns an error or panics produces an empty reply
carrying the error text in `HandlerErrorHeader`. The requester receives a
`*nats.HandlerError` whose `Text` is that text.

A subscriber handler that returns an error is logged.

## Timeouts

`Request` with no context deadline is bounded by the request timeout, and
reports expiry as `nats.ErrTimeout`. A context deadline or cancellation is
reported as the context's own error.

## Concurrency

At most `concurrency` handlers run at once, across all endpoints and
subscribers. Deliveries beyond that wait in nats.go's pending buffer.

## Lifecycle

### The Client

A `Client` owns one NATS connection. `NewClient` opens it and returns the
connected client, and a connection failure is returned from `NewClient`.
`Close` closes the connection, returns nil, and is idempotent. A closed client
is final, and `Request` and `Publish` on it return `ErrClosed`.

### The Runtime and Its Client

A `Runtime` is built from a `Client` the application constructed, and that
client is the runtime's connection. `New` takes a `*nats.Client`, not a
`mesh.Client`, because the runtime subscribes through the NATS connection the
client owns. `New` does not touch the connection.

`Runtime.Client()` returns the client in every state, and the client serves
requests before `Start` as well as during it. `Runtime.ServiceMap()` is the
client's.

### Starting

`Start` subscribes every endpoint and subscriber on the client's connection and
flushes, with a budget of five seconds, so the server knows every subscription
before `Start` returns.

`Start` on a closed client returns `ErrClosed`. A subscribe or flush failure
unsubscribes what was subscribed and leaves the runtime and the client as they
were.

### Stopping

`Stop` unsubscribes, waits for in-flight handlers until its context is done,
cancels the handlers' context, flushes when every handler finished, and closes
the client. It returns the context's error when handlers were abandoned. The
specification's drain in seconds is the context's deadline.

`Stop` on a runtime that is not running returns nil and leaves the client as it
is. Closing the client directly ends the runtime's connection, and a `Stop`
that follows skips the flush and returns the drain result.

A runtime does not restart. `Start` after `Stop` returns `ErrStopped`.

## Errors

nats.go errors are returned unchanged, including connection failures. The
package re-exports the two callers test for most often, `ErrNoResponders` and
`ErrTimeout`, so callers need not import nats.go. The three contract errors,
`mesh.ErrKindMismatch`, `mesh.ErrInvalidTarget`, and
`mesh.ErrNoDeploymentGroup`, come from `mesh`.

These are the errors this package defines.

| Error | Returned when |
|---|---|
| `ErrBadConfig` | A configuration value or per-call option does not parse. It is wrapped with the key and value. |
| `ErrClosed` | `Request` or `Publish` is called on a client after `Close`, which for a runtime's client is after `Stop`, or `Start` is called on a runtime whose client is closed. |
| `ErrAlreadyStarted` | `Start` is called on a running runtime. |
| `ErrStopped` | `Start` is called on a runtime after `Stop`. |
| `*HandlerError` | The serving endpoint handler returned an error or panicked. `Text` is its message. |
