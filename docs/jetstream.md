# JetStream

This transport supports NATS JetStream for topic targets. A publish can wait
for a stream to confirm that it stored the message, and a subscriber can
consume from a JetStream consumer, so a message stays in the stream until a
handler has finished with it.

JetStream is turned on with the `stream` setting, usually carried in the
target's metadata. One client and one runtime carry both core NATS and
JetStream traffic.

## Topic Targets Only

JetStream applies to topic targets only, as endpoints are much less likely
to need durability due to their synchronous nature. JetStream support for
endpoints may be added in the future if a compelling reason for it is found.

A stream must also never cover an endpoint's subject. Such a stream stores
each request and sends its publish ack to the requester. That ack races the
endpoint's reply and usually wins, so the caller reads the ack as the reply
while the handler still runs. When the endpoint is down, the stream still
answers, so the outage goes unnoticed.

These rules follow:

- `stream` in an `Endpoint`'s settings makes `New` return `ErrBadConfig`.
- `stream` in a `Request`'s settings returns `ErrBadConfig`.
- Every runtime checks at `Start` that no stream covers its endpoint
  subjects, as described under [The Boot Check](#the-boot-check).

## Streams

Streams are created outside the runtime, before the processes that use them
start. `Start` fails with the nats.go error when a subscriber's stream does
not exist.

Each JetStream topic target has its own stream, which holds exactly that
target's subject and no wildcards.

A named consumer group's consumer is named after the group. A consumer name
only has to be unique within its stream, and each target has its own stream.
So a group that subscribes to several targets has one consumer on each
target's stream, all with the group's name, and they never collide.

`stream` is a property of the target, carried in its metadata. Publishers
and subscribers do not override it, so every subscriber of a subject consumes
from the stream, and a publish whose stream is missing fails right away.

The stream's retention decides how long it keeps a message:

| Retention | Behavior |
|---|---|
| `limits` | Keeps messages until the stream's age or size limits. |
| `interest` | Keeps a message until every consumer has acked it, and drops messages published while no consumer exists. |
| `workqueue` | Deletes a message once one consumer acks it. It allows one consumer per subject, so one named group or one `none` instance per target. The server enforces this, and its error is returned unchanged. |

## Options

All values are strings, and durations are Go durations such as `"2m"`.

Each setting is read from the first of these that has it:

1. The per-call options of `Request` or `Publish`.
2. The `Endpoint`'s or `Subscriber`'s metadata.
3. The `Target`'s metadata.
4. The default in the table below.

The target's metadata holds the settings declared for the target, and the
other two override them for one call or one subscriber. `stream` and
`request_timeout` are read by `Publish`. The others are read from the
settings of a subscriber that has `stream`, and ignored everywhere else.

| Key | Read by | Default | Meaning | Sent over the connection |
|---|---|---|---|---|
| `stream` | `Publish`, subscriber | none | The stream name. On `Publish` it sends a JetStream publish and waits for the ack. On a subscriber it consumes from a consumer on the stream instead of a queue subscription. | Yes, to the server. On `Publish` it goes in the `Nats-Expected-Stream` header. On a subscriber it names the stream in the runtime's consumer requests. |
| `request_timeout` | `Publish` | the client's `request_timeout` | Bounds the wait for the ack. | No. It bounds how long the client waits for the server's ack. A JetStream publish carries no `Mesh-Timeout`. |
| `create_consumer` | subscriber | `true` | `false` leaves a named group's consumer to be created and updated outside the runtime. | No. It decides whether the runtime sends consumer settings at all. |
| `ack_policy` | subscriber | `double` | `double`, `explicit`, or `none`, described under [Ack Policies](#ack-policies). | Yes, to the server, in the consumer's settings: explicit for `double` and `explicit`, none for `none`. |
| `ack_wait` | subscriber | `30s` | Time allowed before an unacked message is sent again. | Yes, to the server, in the consumer's settings. |
| `max_deliver` | subscriber | the runtime configuration's `max_deliver`, which defaults to `5` | The most times one message is delivered. | Yes, to the server, in the consumer's settings. |
| `backoff` | subscriber | none | Comma-separated delays, one for each delivery in turn, that replace `ack_wait`. `max_deliver` must exceed the number of delays. | Yes, to the server, in the consumer's settings. |
| `nak_delay` | subscriber | none | Delay before a failed message is sent again. Unset sends it again right away. | Yes, to the server, with each nak. |
| `max_ack_pending` | subscriber | `1000` | The most unacked messages the consumer allows at once. | Yes, to the server, in the consumer's settings. |
| `deliver_policy` | subscriber | `all` | `all` or `new`, where a newly created consumer starts in the stream. | Yes, to the server, in the consumer's settings. |
| `consumer_inactive_threshold` | subscriber | `168h` for a named group, `5m` for an ordered consumer, and the server's `5s` for other `none` consumers | How long the consumer outlives its last puller. | Yes, to the server, in the consumer's settings. |
| `pull_max_messages` | subscriber | the runtime configuration's `pull_max_messages`, or `500` | The most messages one pull asks for and the subscriber holds in its buffer. Described under [Pulling](#pulling). | No. It sets the size of each pull request. |
| `pull_max_bytes` | subscriber | the runtime configuration's `pull_max_bytes`, or none | The most bytes one pull asks for and the subscriber holds. Alone, it replaces the message limit. With `pull_max_messages`, it caps the bytes of each batch. It must hold the largest message, or the subscriber stalls. | No. It sets the size of each pull request. |
| `pull_threshold_messages` | subscriber | the runtime configuration's `pull_threshold_messages`, or half of `pull_max_messages` | How few messages still owed from earlier pulls makes the subscriber pull again. | No. It decides when the subscriber pulls. |
| `pull_expiry` | subscriber | the runtime configuration's `pull_expiry`, or `30s` | How long the server keeps a pull open while it has nothing to send. At least `1s`. | No. It sets how long each pull request lasts. |
| `pull_heartbeat` | subscriber | the runtime configuration's `pull_heartbeat`, or half of `pull_expiry`, and `5s` for an ordered consumer | How often the server signals an open pull that has nothing to send. Two missed heartbeats make the subscriber pull again. From `500ms` to `30s`, and no more than half of `pull_expiry`. | No. It is requested with each pull. |

The consumer's settings are sent only when the runtime creates or updates
the consumer, so with `create_consumer = "false"` none of them are sent. No
option is sent as a message header under its own key.

A value that does not parse, or is not one of its allowed values, makes `New`
return `ErrBadConfig`. So does `create_consumer = "false"` on a subscriber
whose `ConsumerGroup` is `none`.

nats.go checks the limits of the pull settings when the subscriber starts
pulling. A pull setting outside them passes `New`, and `Start` returns
nats.go's `jetstream.ErrInvalidOption`.

The consumer settings default to nats.go's defaults, except `max_deliver` and
the inactive threshold of a named group. nats.go's `max_deliver` is
unlimited, which would send a message that never decodes again forever.

## Publishing

A plain `Publish` to a subject a stream covers is stored, with no ack. When
the publish's settings have `stream`, `Publish` sends a JetStream publish with
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
- With `none`, each instance pulls from its own consumer, which nats.go gives
  a new name at each `Start`.

A restarted `none` instance creates a new consumer, which starts where
`deliver_policy` says, and does not resume its old one. An application that
wants a stable consumer per instance gives each instance its own consumer
group.

A `none` subscriber with `ack_policy = "none"` uses nats.go's ordered
consumer. nats.go detects gaps in delivery and replaces the consumer from the
last stream sequence it saw, so no message in the stream is skipped.

Messages from any consumer, including an ordered one, are handed to handlers
that run concurrently, up to the runtime's `concurrency`. Handlers get no
ordering guarantee.

## Pulling

A subscriber asks its consumer for messages in batches of up to
`pull_max_messages`, and asks for more when fewer than
`pull_threshold_messages` are still owed. Messages that arrive while every
handler slot is busy wait in the subscriber's buffer.

The server counts a buffered message as delivered and unacked:

- Its `ack_wait` is already running. `Mesh-Deadline` is measured from when
  the message leaves the buffer, so it can fall after the server sends the
  message again.
- It counts against `max_ack_pending`.
- In a named group, other instances cannot take it, even when they are idle.

With the default of 500 and a `concurrency` of a few handlers, most of each
batch waits in the buffer. A `pull_max_messages` close to `concurrency` keeps
each handler busy while holding few messages back.

## Ack Policies

`ack_policy` decides how the runtime settles each message:

- `double` acks each message and waits for the server to confirm the ack.
- `explicit` acks each message without waiting.
- `none` acks nothing, and nothing is sent again, so delivery is at most once.

With `none`, `ack_wait`, `backoff`, `max_deliver`, `nak_delay`,
`max_ack_pending`, and `Mesh-Deadline` do not apply.

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

Handlers need to be idempotent, because a message can be handled more than
once after a missed deadline, a failed ack, or a nak at the end of a drain.
`ack_policy = "double"` confirms that each ack was recorded.

## The Handler's Time Limit

A handler that runs past its ack deadline gets its message delivered again
while it is still running. The transport tells the handler its deadline:

- The handler's context carries the deadline.
- The message metadata carries it as `Mesh-Deadline`, in RFC 3339 with
  fractional seconds.

The deadline is the delivery time plus the consumer's `ack_wait`, or the
`backoff` entry for this delivery when the consumer has one. The runtime
reads these from the server's settings for the consumer at `Start`.

Cancellation is cooperative, so a handler that ignores its context keeps
running.

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
  subscriber's settings and the group's name.
- A named group's consumer filters on the target's subject.
- A `none` subscriber's consumer is created with no name, and nats.go names
  it.
- The runtime reads each consumer's settings back from the server, so
  `Mesh-Deadline` uses the server's values.

Because every `Start` writes the settings, these effects follow:

- During a rolling deploy, old and new instances rewrite each other's
  consumer settings until the deploy finishes.
- Instances of a group whose settings differ keep rewriting the consumer
  while they all run, so every instance of a group needs the same consumer
  settings.
- A change to a setting the server does not allow to change after creation,
  such as `deliver_policy`, makes `Start` fail with the server's error.
  Applying the change means deleting the consumer by hand, which loses its
  position in the stream.
- Under `interest` retention, messages published before any consumer exists
  are dropped.

### Orphaned Consumers

A consumer whose subscriber is removed from code stays on the server, and
under `interest` retention it keeps every message it has not acked.

A named group's consumer expires once `consumer_inactive_threshold` passes
with no pulls, which defaults to 7 days. A running runtime pulls
continuously, so a consumer reaches the threshold only when every instance of
its group has stopped. A group that stays down longer than the threshold
loses its consumer, and its next `Start` creates a new one under
`deliver_policy`. Under `interest` retention, the messages that no other
consumer still needed are gone by then.

### Consumers Managed Outside the Runtime

With `create_consumer = "false"`, the runtime creates and updates nothing for
a named group's consumer. At `Start` it looks up the consumer named by the
group, and fails with the nats.go error when it does not exist. It reads the
consumer's settings from the server and ignores the subscriber's consumer
settings.

## Inspecting Streams and Consumers

The client and the runtime give access to JetStream state outside a handler.

| Name | Returns |
|---|---|
| `(*Client).Conn()` | The client's `*nats.Conn`. |
| `(*Client).JetStream()` | The `jetstream.JetStream` the client publishes with. |
| `(*Client).Stream(ctx, target)` | The stream named by the target's `stream`, or nil and no error when the target has none. |
| `(*Runtime).Receiver(target)` | The runtime's `*nats.Receiver` for the target's endpoint or subscriber while the runtime is running, or false when it is not running or has neither for the target. |
| `(*Receiver).Target()` | The receiver's target. |
| `(*Receiver).ConsumerGroup()` | The resolved consumer group. |
| `(*Receiver).Stream(ctx)` | The stream named by the receiver's settings, or nil and no error when they have no `stream`, which is so for every endpoint. |
| `(*Receiver).Consumer()` | The `jetstream.Consumer` the receiver pulls from, or nil when its settings have no `stream`. |

`Client.Stream` reads the target, so it names the stream a publish to the
target uses. A receiver's `Stream` reads the receiver's settings, so it
names the stream the subscriber consumes. `Client.Stream` and
`Receiver.Stream` return `ErrClosed` after the client is closed, and a
lookup failure as the nats.go error unchanged.

For an ordered or `none` consumer, `Receiver.Consumer` is the only way to
reach the consumer, because nats.go names it at each `Start`. For an ordered
consumer, `Info` describes the consumer nats.go is using at the time of the
call.

A process that loads everything in a stream before it serves can wait on
the subscriber's consumer after `Start`:

```go
rc, _ := rt.Receiver(target)
for {
    info, err := rc.Consumer().Info(ctx)
    if err != nil { /* ... */ }
    if info.NumPending == 0 && info.NumAckPending == 0 {
        break
    }
    time.Sleep(100 * time.Millisecond)
}
```

With `double` or `explicit` acks, no pending and no unacked messages means
every message up to that point was handled. With `ack_policy = "none"`,
nothing is acked, so the check shows only that every message was delivered,
while handlers may still be running.

`Conn` returns the connection the client and the runtime use. Closing or
draining it directly ends the client, and for a runtime built from the
client, its subscriptions and consumers. `Request` and `Publish` then return
the nats.go error rather than `ErrClosed`.

## The Boot Check

`Start` looks up each endpoint subject's stream. When a stream covers one,
`Start` returns `ErrStreamCoversRoute`, wrapped with the subject and the
stream name.

The settings rules catch `stream` on an endpoint or a request. The boot
check catches a stream already on the server that covers an endpoint
subject, such as one created by hand with a wildcard subject. Such a stream
answers plain core requests, whatever settings the caller and the endpoint
use.

A server without JetStream has no streams, so the check passes. A runtime
with no endpoints makes no lookups.

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
