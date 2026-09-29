# What the NATS Runtime Decides


The specification leaves these to each transport. Full detail is in the
`nats` package documentation.

**Targets.** Segments join with `.` into a subject. A segment must be
non-empty and free of `.`, `*`, `>`, whitespace, and non-printable
characters. Targets are literal and hold no wildcards.

**Configuration.** All values are strings. `NewClient` reads the connection
keys and `New` reads `deployment_group`, which is required, and
`concurrency`. Each constructor ignores the other's keys, so one `Config`
can be given to both.

| key                | read by     | default            | meaning                                            |
|--------------------|-------------|--------------------|----------------------------------------------------|
| `url`              | `NewClient` | `nats://127.0.0.1:4222` | server URL or comma-separated list            |
| `name`             | `NewClient` | none               | connection name reported to the server             |
| `connect_timeout`  | `NewClient` | `5s`               | bound on the initial connection, Go duration       |
| `request_timeout`  | `NewClient` | `30s`              | bound on `Request` when ctx has no deadline, Go duration; also accepted as a per-call option |
| `deployment_group` | `New`       | required           | the queue group endpoints and subscribers join     |
| `concurrency`      | `New`       | CPU count          | max handlers running at once                       |

A value that does not parse yields `ErrBadConfig`. A logger is passed to
`New` as `nats.WithLogger`. The package re-exports `ErrNoResponders` and
`ErrTimeout` from nats.go so callers need not import it.

**Metadata.** Message metadata rides as NATS headers, one value per key. The
runtime reads no message keys and writes `HandlerErrorHeader` on a failed
reply. `Endpoint` and `Subscriber` metadata is read at construction. `consumer_group` is taken
from the `Endpoint` or `Subscriber` first, then from its `Target`.
`deployment_group` on a client-side `Target` is ignored.

**Delivery.** A consumer group is a NATS queue group. Every endpoint and
subscriber joins the deployment group unless `consumer_group` overrides it.
`none` gives a plain subscription, so every instance handles every message,
and for an endpoint every instance replies. Two endpoints or subscribers on one subject are
two NATS subscriptions, with whatever delivery NATS gives them.

**Handler failure.** An endpoint handler that returns an error or panics
produces an empty reply carrying the error text in `HandlerErrorHeader`; the
requester receives `*nats.HandlerError`. A failing subscriber handler is
logged.

**Timeouts.** `Request` with no context deadline is bounded by the request
timeout and reports expiry as `nats.ErrTimeout`. A context deadline or
cancellation is reported as the context's own error.

**Concurrency.** At most `concurrency` handlers run at once across all
endpoints and subscribers. Beyond that, deliveries wait in nats.go's pending buffer.

**Lifecycle.** A `Client` owns a NATS connection. `NewClient` returns a
connected one, and its `Close` closes the connection; `Close` is idempotent.
A `Runtime` is built from a `Client` the application constructed, and that
client is the runtime's connection. `New` takes a `*nats.Client`, not a
`mesh.Client`, because the runtime subscribes through the NATS connection the
client owns. `Runtime.Client()` returns it in every state, and
`Runtime.ServiceMap()` is the client's. `Start` subscribes every endpoint and subscriber on
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
