# JetStream

This transport supports NATS JetStream for topic targets. A publish can wait
for a stream to confirm that it stored the message, and a subscriber can
consume from a JetStream consumer, so a message stays in the stream until a
handler has finished with it.

JetStream is turned on per call and per subscriber with the `stream` option.
One client and one runtime carry both core NATS and JetStream traffic.

## Topic Targets Only

JetStream applies to topic targets. A stream never covers a route subject or
an inbox subject.

A stream that covers a route subject stores every request and answers the
requester with its publish ack. That ack races the endpoint's reply and
usually wins, so the caller reads the ack as the reply while the handler
still runs. A stopped endpoint also goes unnoticed, because the stream
answers in its place.

These rules follow:

- `stream` on an `Endpoint` makes `New` return `ErrBadConfig`.
- `stream` in the options of a `Request` returns `ErrBadConfig`.
- A runtime with a JetStream subscriber checks at `Start` that no stream
  covers its endpoint subjects, as described under
  [The Boot Check](#the-boot-check).

## Streams

Streams are created outside the runtime, before the processes that use them
start. `Start` fails with the nats.go error when a subscriber's stream does
not exist.

Each JetStream topic target has its own stream, which holds exactly that
target's subject and no wildcards. Consumer names are scoped to their stream,
which is what lets a consumer take its consumer group's name.

`stream` is a property of the target. Every publisher and subscriber of a
target sets the same `stream`, so all of a subject's subscribers consume from
the stream, and a publish whose stream is missing fails right away.

The stream's retention decides how long it keeps a message:

| Retention | Behavior |
|---|---|
| `limits` | Keeps messages until the stream's age or size limits. |
| `interest` | Keeps a message until every consumer has acked it, and drops messages published while no consumer exists. |
| `workqueue` | Deletes a message once one consumer acks it. It allows one consumer per subject, so one named group or one `none` instance per target. The server enforces this, and its error is returned unchanged. |

## Options

All values are strings. `stream` is read from `Publish` options and from
subscriber metadata. The others are read from the metadata of a subscriber
that sets `stream`, and ignored everywhere else. Durations are Go durations
such as `"2m"`.

| Key | Read by | Default | Meaning |
|---|---|---|---|
| `stream` | `Publish`, subscriber | none | The stream name. On `Publish` it sends a JetStream publish and waits for the ack. On a subscriber it consumes from a consumer on the stream instead of a queue subscription. |
| `request_timeout` | `Publish` | the client's `request_timeout` | Bounds the wait for the ack. |
| `create_consumer` | subscriber | `true` | `false` leaves a named group's consumer to be created and updated outside the runtime. |
| `ack_policy` | subscriber | `double` | `double`, `explicit`, or `none`, described under [Ack Policies](#ack-policies). |
| `ack_wait` | subscriber | `30s` | Time allowed before an unacked message is sent again. |
| `max_deliver` | subscriber | the runtime configuration's `max_deliver`, which defaults to `5` | The most times one message is delivered. |
| `backoff` | subscriber | none | Comma-separated delays, one for each delivery in turn, that replace `ack_wait`. `max_deliver` must exceed the number of delays. |
| `nak_delay` | subscriber | none | Delay before a failed message is sent again. Unset sends it again right away. |
| `max_ack_pending` | subscriber | `1000` | The most unacked messages the consumer allows at once. |
| `deliver_policy` | subscriber | `all` | `all` or `new`, where a newly created consumer starts in the stream. |
| `consumer_inactive_threshold` | subscriber | `168h` for a named group, `5m` for an ordered consumer, and the server's `5s` for other `none` consumers | How long the consumer outlives its last puller. |

A value that does not parse, or is not one of its allowed values, makes `New`
return `ErrBadConfig`. So does `create_consumer = "false"` on a subscriber
whose consumer group is `none`.

The consumer settings default to nats.go's defaults, except `max_deliver` and
the inactive threshold of a named group. nats.go's `max_deliver` is
unlimited, which would send a message that never decodes again forever.

## Publishing

A plain `Publish` to a subject a stream covers is stored, with no ack. When
the options set `stream`, `Publish` sends a JetStream publish with
`Nats-Expected-Stream` set to the stream, so a subject that lands in a
different stream fails. It waits for the ack, bounded by `request_timeout` or
the context's deadline, whichever comes first.

`Publish` returns nil once the ack arrives, and does not return the stream
sequence. A publish the stream drops as a duplicate also counts as success.

`Mesh-Message-Id` sets `Nats-Msg-Id`, so the stream drops a repeat of the same
message within its duplicate window. When a message carries both with
different values, `Mesh-Message-Id` wins.

An error carried in the ack, or from nats.go, is returned unchanged. What a
missing stream returns depends on what else listens on the subject:

| Stream | Core subscribers | Result |
|---|---|---|
| Yes | any | The ack, or the error carried in the ack |
| No | none | `jetstream.ErrNoStreamResponse`, after two retries |
| No | one or more | `nats.ErrTimeout` once `request_timeout` passes |

## Subscribing

A subscriber with `stream` consumes from a pull consumer on that stream,
starting at `Start`. Its resolved consumer group decides who shares a
consumer:

- All instances in a named group pull from one consumer named by the group,
  so each message goes to one instance of the group, and each group gets
  every message.
- With `none`, each instance pulls from its own consumer, which nats.go names
  anew at each `Start`.

A restarted `none` instance creates a new consumer, which starts where
`deliver_policy` says, and does not resume its old one. An application that wants a stable consumer per instance gives each instance
its own consumer group.

A `none` subscriber with `ack_policy = "none"` uses nats.go's ordered
consumer. nats.go detects gaps in delivery and replaces the consumer from the
last stream sequence it saw, so no message in the stream is skipped.

Messages from any consumer, including an ordered one, are handed to handlers
that run concurrently, up to the runtime's `concurrency`. Handlers get no
ordering guarantee.

## Ack Policies

`ack_policy` decides how the runtime settles each message:

- `double` acks each message and waits for the server to confirm the ack.
- `explicit` acks each message without waiting.
- `none` acks nothing, and nothing is sent again. Delivery is at most once,
  and `ack_wait`, `backoff`, `max_deliver`, `nak_delay`, `max_ack_pending`,
  and `Mesh-Deadline` do not apply.

`double` and `explicit` create the consumer with NATS's explicit ack policy.
With `create_consumer = "false"`, the server's ack policy decides whether
messages are acked, and `double` or `explicit` decides only whether the
runtime waits for the confirmation.

NATS's `all` ack policy is not offered. Acking one message under `all` acks
every earlier message the consumer delivered, across all of a group's
instances, including messages whose handlers are still running or about to
fail.

## Handler Results

A handler that returns nil is acked. A handler that returns an error or
panics is logged and nacked. The nak sends the message again right away, or
after `nak_delay` when it is set. `max_deliver` bounds how many times one
message is delivered, and `backoff` spaces out redelivery after a missed ack
deadline.

Handlers are expected to be idempotent. A message can be handled more than
once after a missed deadline, a failed ack, or a nak at the end of a drain.
`ack_policy = "double"` confirms that each ack was recorded.

## The Handler's Time Limit

A handler that runs past its ack deadline gets its message delivered again
while it is still running. The transport tells the handler its deadline:

- The deadline is the delivery time plus the consumer's `ack_wait`, or the
  `backoff` entry for this delivery when the consumer has one. The runtime
  reads these from the server's settings for the consumer at `Start`.
- The handler's context carries the deadline. Cancellation is cooperative,
  so a handler that ignores its context keeps running.
- The message metadata carries it as `Mesh-Deadline`, in RFC 3339 with
  fractional seconds.

A consumer that does not ack has no deadline.

## Delivery Metadata

Message headers reach handlers as metadata, including headers such as
`Nats-Msg-Id`. When a message carries `Nats-Msg-Id`, the transport also sets
`Mesh-Message-Id` to its value.

JetStream's delivery details are not headers. The transport copies them into
the metadata of each delivery:

| Key | Value |
|---|---|
| `Mesh-Delivery-Attempt` | Which delivery of this message this is, starting at 1. |
| `Mesh-Deadline` | The ack deadline, on deliveries from a consumer that acks. |
| `Mesh-Nats-Stream` | The stream name. |
| `Mesh-Nats-Consumer` | The consumer name. |
| `Mesh-Nats-Stream-Sequence` | The message's sequence in the stream. |
| `Mesh-Nats-Consumer-Sequence` | The delivery's sequence in the consumer. |
| `Mesh-Nats-Num-Pending` | How many messages are still waiting in the consumer. |
| `Mesh-Nats-Timestamp` | When the stream stored the message, in RFC 3339. |

The `Mesh-Nats-` keys are also exported as constants, such as
`nats.StreamMetadataKey`. The prefix keeps them out of the `Nats-` header
space, which the server reads, in case a handler copies its metadata onto a
message it sends.

## Consumers Created by the Runtime

The runtime creates every consumer itself at `Start`, because only the
running process knows each subscriber's resolved consumer group.

- A named group's consumer is created, or updated when it exists, with the
  subscriber's settings and the group's name. Its filter subject is the
  target's subject.
- A `none` subscriber's consumer is created with no name, and nats.go names
  it.
- The runtime reads each consumer's settings back from the server, so
  `Mesh-Deadline` uses the server's values.

Because every `Start` writes the settings, these effects follow:

- During a rolling deploy, old and new instances rewrite each other's
  consumer settings until the deploy finishes.
- All instances of a group should resolve the same consumer settings.
  Instances whose settings differ keep rewriting the consumer while they all
  run.
- A setting the server does not allow to change after creation, such as
  `deliver_policy`, makes `Start` fail with the server's error when it
  changes. The consumer is then deleted by hand, which loses its position in
  the stream.
- Under `interest` retention, messages published before any consumer exists
  are dropped.

### Orphaned Consumers

A consumer whose subscriber is removed from code stays on the server, and
under `interest` retention it keeps every message it has not acked.

A named group's consumer expires once `consumer_inactive_threshold` passes
with no pulls, which defaults to 7 days. A running runtime pulls
continuously, so only a consumer whose group has stopped running entirely
reaches it. A group that stays down longer than the threshold loses its
consumer. Its next `Start` creates a new one under `deliver_policy`, and under
`interest` retention the messages only it still needed are gone.

### Consumers Managed Outside the Runtime

With `create_consumer = "false"`, the runtime creates and updates nothing for
a named group's consumer. At `Start` it looks up the consumer named by the
group, and fails with the nats.go error when it does not exist. It reads the
consumer's settings from the server and ignores the subscriber's consumer
settings.

## The Boot Check

When a runtime has at least one JetStream subscriber, `Start` looks up each
endpoint subject's stream. When a stream covers one, `Start` returns
`ErrStreamCoversRoute`, wrapped with the subject and the stream name. A
runtime with no JetStream subscriber skips the check.

## Stopping

`Stop` stops pulling when it begins, and the messages already pulled are
handled during the drain. When the drain ends first, `Stop` cancels the
handlers' context and naks every message that is not settled, whether its
handler had not started or was still running. A message nacked this way can
be handled twice. A consumer with `ack_policy = "none"` has nothing to nak.

The runtime logs a warning through its `*slog.Logger` for each message it
naks this way, with these attributes:

- `subject`: the message's subject.
- `stream`: the stream name.
- `consumer`: the consumer name.
- `stream_sequence`: the message's sequence in the stream, which identifies
  it even when the publisher set no ID.
- `message_id`: the message's `Mesh-Message-Id`, when it has one.
- `handler_running`: `true` when its handler was still running, and `false`
  when it had not started.
