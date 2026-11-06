# Local Behavior Runbook

This runbook checks the transport's behavior by hand against a local
`nats-server` with JetStream. Each check runs a receiving process in one
terminal and a sending process in another, and lists the output that shows
the behavior.

Three small programs in `docs/runbooks/cmd/` do the work. They are part of the
module, so they build with the current checkout and need no setup.

| Program | Does |
|---|---|
| `serve` | Runs one endpoint or subscriber and prints each message its handler receives, with its metadata and context deadline. It also prints a JetStream subscriber's consumer settings as the server holds them, and stops through `Stop` on Ctrl-C. |
| `send` | Sends requests or publishes and prints each result, naming the error when there is one. |
| `streams` | Creates, inspects, and deletes streams and consumers. |

`serve` and `send` take the settings the library reads as repeatable
`key=value` flags, one flag for each place a setting can live:

| Flag | Sets | Read by |
|---|---|---|
| `-cfg` | The `mesh.Config` given to `NewClient` and `New` | Both |
| `-target` | The `Target`'s metadata | Both |
| `-set` | The `Endpoint`'s or `Subscriber`'s metadata in `serve`, and the per-call options in `send` | Both |
| `-md` | The message metadata | `send` |

These are the other flags.

| Program | Flag | Default | Meaning |
|---|---|---|---|
| `serve`, `send` | `-kind` | `topic` | `route` for an endpoint and `Request`, `topic` for a subscriber and `Publish`. |
| `serve`, `send` | `-subject` | `demo.created` | The target's subject. |
| `serve` | `-group` | empty | The `ConsumerGroup` field: empty for the deployment group, `none`, or a name. |
| `serve` | `-fail` | `0` | Fail the first N handler calls. |
| `serve` | `-work` | `0` | How long each handler call runs. It stops early when the handler's context is done. |
| `serve` | `-ignore-ctx` | off | Keep running for the whole `-work` after the handler's context is done, like a handler that ignores its deadline. |
| `serve` | `-drain` | `10s` | The drain budget `Stop` gets on Ctrl-C. |
| `send` | `-n` | `1` | How many messages to send. |
| `send` | `-payload` | `hello` | The payload. Each message appends its number. |
| `send` | `-deadline` | `0` | A context deadline for each call. |
| `send` | `-stream-info` | off | After sending, print the target's stream from `Client.Stream`. |

`serve` uses the deployment group `demo` unless `-cfg deployment_group=...`
sets another. `NATS_URL` points all three programs at a server other than
`nats://127.0.0.1:4222`.

## Terminal 1, the Server

```sh
brew install nats-server   # once
nats-server -js -sd "$(mktemp -d)"
```

`-js` turns on JetStream, and `-sd` keeps its storage in a fresh temporary
directory, so every session starts with no streams and no consumers.

The checks below run from the repository root in terminals 2 and 3. Each
`serve` keeps running until Ctrl-C.

## Requests

### Mesh-Timeout, Mesh-Deadline, and the Handler's Context

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -kind route -subject demo.echo
# terminal 3
go run ./docs/runbooks/cmd/send -kind route -subject demo.echo -cfg request_timeout=2s
go run ./docs/runbooks/cmd/send -kind route -subject demo.echo -deadline 500ms
```

Terminal 3 prints `reply "echo: hello 1" metadata map[Served-By:<pid>]` both
times. Terminal 2 shows what the handler received:

```
    Mesh-Deadline: 2026-11-03T15:49:01.06138-07:00
    Mesh-Timeout: 2000
    context deadline: 2026-11-03T15:49:01.06138-07:00 (in 2s)
...
    Mesh-Timeout: 499
    context deadline: ... (in 499ms)
```

The second request's context deadline comes sooner than `request_timeout`, so
it bounds the request and sets `Mesh-Timeout`.

### Where request_timeout Is Read From

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -kind route -subject demo.slow -work 3s
# terminal 3
go run ./docs/runbooks/cmd/send -kind route -subject demo.slow -target request_timeout=500ms
go run ./docs/runbooks/cmd/send -kind route -subject demo.slow -target request_timeout=500ms -set request_timeout=4s
```

The first prints `nats.ErrTimeout after 502ms`, because the target's setting
applies. The second prints a reply after about 3s, because the per-call
option outranks the target.

### Request Errors

```sh
# terminal 3, with nothing serving demo.none
go run ./docs/runbooks/cmd/send -kind route -subject demo.none
# terminal 2
go run ./docs/runbooks/cmd/serve -kind route -subject demo.fail -fail 1
# terminal 3
go run ./docs/runbooks/cmd/send -kind route -subject demo.fail -n 2
go run ./docs/runbooks/cmd/send -kind route -subject demo.fail -set stream=X
```

| Command | Prints |
|---|---|
| Nothing serving | `nats.ErrNoResponders` |
| `-fail 1`, first request | `HandlerError "failing call 1 of 1 on purpose"` |
| `-fail 1`, second request | `reply "echo: hello 2"` |
| `-set stream=X` | `nats.ErrBadConfig: ... stream="X" applies to topic targets only` |

## Core Consumer Groups

```sh
# terminals 2, 4, and 5
go run ./docs/runbooks/cmd/serve -subject demo.core -cfg deployment_group=billing
go run ./docs/runbooks/cmd/serve -subject demo.core -cfg deployment_group=audit
go run ./docs/runbooks/cmd/serve -subject demo.core -cfg deployment_group=audit
# terminal 3
go run ./docs/runbooks/cmd/send -subject demo.core -n 4
```

`billing` handles all four messages. The two `audit` instances share a queue
group and handle four between them.

Adding `-group none` to the two `audit` instances makes each of them handle
all four. Adding `-group shared` to all three makes the three share one
group across both deployments, so the four messages are spread across all
three instances.

## Configuration Mistakes

Each of these exits right away with `nats.ErrBadConfig` and the key and value.

```sh
go run ./docs/runbooks/cmd/send -cfg request_timeout=soon
go run ./docs/runbooks/cmd/serve -cfg concurrency=many
go run ./docs/runbooks/cmd/serve -cfg max_deliver=0
go run ./docs/runbooks/cmd/serve -set stream=DEMO_CREATED -set ack_policy=all
go run ./docs/runbooks/cmd/serve -group none -set stream=DEMO_CREATED -set create_consumer=false
go run ./docs/runbooks/cmd/serve -cfg pull_expiry=soon
go run ./docs/runbooks/cmd/serve -set stream=DEMO_CREATED -set pull_max_messages=0
```

For example, the fourth prints
`New: nats: bad configuration value: ack_policy="all" must be "double", "explicit", or "none"`.

## Streams

The JetStream checks use one stream for the subject `demo.created`:

```sh
go run ./docs/runbooks/cmd/streams add DEMO_CREATED demo.created
```

It prints `created stream "DEMO_CREATED" for [demo.created] with Limits retention`.
`streams add NAME SUBJECT interest` or `workqueue` creates a stream with that
retention instead.

## The Boot Check

```sh
go run ./docs/runbooks/cmd/streams add BAD demo.echo
go run ./docs/runbooks/cmd/serve -kind route -subject demo.echo
go run ./docs/runbooks/cmd/streams rm BAD
```

`serve` exits with
`Start: nats: a stream covers an endpoint subject: subject "demo.echo", stream "BAD"`.
After `rm`, the same `serve` starts.

## JetStream Publishing

With nothing subscribed:

```sh
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED -n 2 -md Mesh-Message-Id=same-id
go run ./docs/runbooks/cmd/streams info DEMO_CREATED
go run ./docs/runbooks/cmd/send -subject demo.created -set stream=OTHER
go run ./docs/runbooks/cmd/send -subject demo.nostream -set stream=NOPE
```

| Command | Prints |
|---|---|
| A publish to the stream | `publish 1: ok` |
| Two publishes with the same `Mesh-Message-Id` | `ok` twice, while `streams info` shows only one more message, because the stream dropped the repeat |
| The subject is in a different stream | `API error: code=400 err_code=10060 description=expected stream does not match` |
| No stream holds the subject | `jetstream.ErrNoStreamResponse after 503ms`, after nats.go's two retries |

With a core subscriber on the subject and no stream, the publish waits for an
ack that never comes:

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -subject demo.listen
# terminal 3
go run ./docs/runbooks/cmd/send -subject demo.listen -set stream=NOPE -set request_timeout=1s
```

It prints `nats.ErrTimeout after 1.001s`.

`Client.Stream` reads only the target's metadata:

```sh
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED -stream-info
go run ./docs/runbooks/cmd/send -target stream=DEMO_CREATED -stream-info
```

The first prints `Client.Stream: the target has no stream setting`. The
second prints `stream "DEMO_CREATED": N messages, last sequence N, M consumers`.

## JetStream Subscribing

Each check below uses its own consumer group, so the checks don't share a
consumer. `-set deliver_policy=new` keeps a new consumer from replaying what
earlier checks published.

### A Named Group's Consumer and Its Settings

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -group audit -set stream=DEMO_CREATED -set ack_wait=10s -fail 1
# terminal 3
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED -md Mesh-Message-Id=abc
```

At start, terminal 2 prints the consumer as the server holds it:

```
consumer group: audit
JetStream consumer "audit" on stream "DEMO_CREATED": ack_policy=AckExplicit ack_wait=10s max_deliver=5 backoff=[] max_ack_pending=1000 deliver_policy=all inactive_threshold=168h0m0s
```

`max_deliver=5` comes from the runtime's default, and the 7-day
`inactive_threshold` is the named-group default. The first call fails and is
nacked, and the message comes again right away:

```
call 1: payload "hello 1"
    Mesh-Deadline: 2026-11-03T15:48:54.823665-07:00
    Mesh-Delivery-Attempt: 1
    Mesh-Message-Id: abc
    Mesh-Nats-Consumer: audit
    Mesh-Nats-Consumer-Sequence: 1
    Mesh-Nats-Num-Pending: 0
    Mesh-Nats-Stream: DEMO_CREATED
    Mesh-Nats-Stream-Sequence: 1
    Mesh-Nats-Timestamp: 2026-11-03T15:48:44.823405-07:00
    Nats-Expected-Stream: DEMO_CREATED
    Nats-Msg-Id: abc
    context deadline: 2026-11-03T15:48:54.823665-07:00 (in 10s)
call 1: returning an error
ERROR nats: subscriber handler failed subject=demo.created stream=DEMO_CREATED error="failing call 1 of 1 on purpose"
call 2: payload "hello 1"
    Mesh-Delivery-Attempt: 2
    ...
```

`Mesh-Deadline` and the context deadline are the delivery time plus
`ack_wait`. `streams consumers DEMO_CREATED` afterwards shows the consumer
with `ack pending 0`.

### Instances of One Group Split Messages

```sh
# terminals 2 and 4
go run ./docs/runbooks/cmd/serve -group split -set stream=DEMO_CREATED -set deliver_policy=new
# terminal 3
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED -n 10
```

Each instance handles some of the ten, and no message reaches both. A third
`serve` with `-group other` handles all ten.

### Ack Policies

`double` and `explicit` create the same consumer on the server, with NATS's
explicit ack policy. They differ in how the runtime sends each ack: `double`
sends it as a request and waits for the server to confirm it, and `explicit`
publishes it without waiting. A plain subscriber on the stream's ack subjects
shows the difference, because only a request carries a reply subject. This
is the one check that uses the `nats` command-line tool, installed with
`brew install nats-io/nats-tools/nats`.

```sh
# terminal 4
nats sub '$JS.ACK.DEMO_CREATED.>'
# terminal 2
go run ./docs/runbooks/cmd/serve -group ack-double -set stream=DEMO_CREATED -set deliver_policy=new -set ack_policy=double
# terminal 3
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED
```

Terminal 4 prints the ack with a reply subject:

```
[#1] Received on "$JS.ACK.DEMO_CREATED.ack-double.1.1.1.1793761149456144000.0" with reply "_INBOX.ENt599afC8rSqx8U5lXjWd.zy7XFW4q"
+ACK
```

The same steps with `-group ack-explicit -set ack_policy=explicit` print the
ack with no reply subject:

```
[#1] Received on "$JS.ACK.DEMO_CREATED.ack-explicit.1.2.1.1793761152391687000.0"
+ACK
```

With either policy, a failing handler's message is nacked and comes again.
With `-fail 1` added, terminal 2 prints `call 1` with
`Mesh-Delivery-Attempt: 1` and then `call 2` with `Mesh-Delivery-Attempt: 2`.

`none` on a named group acks nothing, so delivery is at most once:

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -group once -set stream=DEMO_CREATED -set deliver_policy=new -set ack_policy=none -fail 1
# terminal 3
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED
```

Terminal 2 prints the consumer with `ack_policy=AckNone max_deliver=-1`, then
one `call 1` with `context deadline: none`, which fails and is not delivered
again. Terminal 4, if still running, prints no ack.

`none` on a subscriber whose `ConsumerGroup` is `none` gives an ordered
consumer, as described under
[none Consumers and Ordered Consumers](#none-consumers-and-ordered-consumers).

### Retries: max_deliver, nak_delay, and backoff

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -group retry -set stream=DEMO_CREATED -set deliver_policy=new \
  -set max_deliver=3 -set nak_delay=1s -fail 99
# terminal 3
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED
```

The message arrives three times, a second apart, with `Mesh-Delivery-Attempt`
1, 2, and 3, and then stops.

A failed handler's message is nacked, and a nak without `nak_delay` sends it
again right away. `backoff` applies only when a delivery is not settled before
its deadline, so it shows with a handler that runs past its deadline:

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -group bo -set stream=DEMO_CREATED -set deliver_policy=new \
  -set backoff=2s,10s -set max_deliver=3 -work 30s -ignore-ctx -drain 1s
# terminal 3
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED
```

The consumer prints `ack_wait=2s max_deliver=3 backoff=[2s 10s]`, because
the server sets `ack_wait` to the first `backoff` entry. The message arrives
again 2s after the first delivery and 10s after the second, while the earlier
handlers are still running:

```
17:02:35 call 1: payload "hello 1"
    Mesh-Delivery-Attempt: 1
17:02:37 call 2: payload "hello 1"
    Mesh-Delivery-Attempt: 2
17:02:47 call 3: payload "hello 1"
    Mesh-Delivery-Attempt: 3
```

Each delivery's context deadline is its arrival plus its `backoff` entry. The
same command with `-fail 99` in place of `-work 30s -ignore-ctx` delivers all
three right away, because each failure is nacked before its deadline.

### Consumer Settings the Server Won't Change

The runtime creates or updates a named group's consumer at every `Start`. The
server refuses some changes to a consumer after it is created, and
`deliver_policy` is one of them.

```sh
go run ./docs/runbooks/cmd/serve -group drain -set stream=DEMO_CREATED -set deliver_policy=new
# Ctrl-C, then start the same group without deliver_policy=new
go run ./docs/runbooks/cmd/serve -group drain -set stream=DEMO_CREATED
```

The second exits with
`Start: nats: API error: code=500 err_code=10012 description=deliver policy can not be updated`.

### none Consumers and Ordered Consumers

```sh
go run ./docs/runbooks/cmd/serve -group none -set stream=DEMO_CREATED
go run ./docs/runbooks/cmd/serve -group none -set stream=DEMO_CREATED -set ack_policy=none
```

The first gets its own ephemeral consumer with a name nats.go generates, and
acks. The second is an ordered consumer:

```
consumer group: none
JetStream consumer "v69cCq8dEN2sSbaIs3MDTF_1" on stream "DEMO_CREATED": ack_policy=AckNone ack_wait=0s max_deliver=-1 backoff=[] max_ack_pending=0 deliver_policy=all inactive_threshold=5m0s
```

With `deliver_policy=all` it replays the whole stream, and its handlers get no
context deadline and no `Mesh-Deadline`. Each start creates a new consumer
for both commands.

### A Consumer Managed Outside the Runtime

```sh
go run ./docs/runbooks/cmd/streams add-consumer DEMO_CREATED billing demo.created
go run ./docs/runbooks/cmd/serve -group billing -set stream=DEMO_CREATED -set create_consumer=false -set ack_wait=5s
go run ./docs/runbooks/cmd/serve -group nobody -set stream=DEMO_CREATED -set create_consumer=false
```

The first `serve` prints `ack_wait=1m0s`, which is the consumer's own setting,
because the runtime ignores `-set ack_wait=5s` for a consumer it doesn't
create. The second exits with
`Start: nats: API error: code=404 err_code=10014 description=consumer not found`.

### Target Settings and Subscriber Overrides

```sh
go run ./docs/runbooks/cmd/serve -group layer -target stream=MISSING -set stream=DEMO_CREATED
go run ./docs/runbooks/cmd/serve -group layer2 -target stream=MISSING
```

The first consumes from `DEMO_CREATED`, because the subscriber's own setting
outranks the target's. The second exits with
`Start: nats: API error: code=404 err_code=10059 description=stream not found`.

### Pull Settings

The pull settings decide how many messages a subscriber takes from its
consumer before its handlers are ready for them. With a handler that holds
the only slot, that number shows as ack pending, because the server counts
every message the subscriber has taken as unacked.

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -group pull -set stream=DEMO_CREATED -set deliver_policy=new \
  -cfg concurrency=1 -work 60s -set ack_wait=60s -drain 1s
# terminal 3
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED -n 20
go run ./docs/runbooks/cmd/streams consumers DEMO_CREATED
```

With nats.go's default of 500, the subscriber takes all 20:

```
consumer "pull": pending 0, ack pending 20, redelivered 0, ...
```

Ctrl-C in terminal 2, then repeat with a new `-group` name for each line
below, adding its flags to the `serve` command:

| `serve` flags | Ack pending |
|---|---|
| `-set pull_max_messages=3` | 3 |
| `-cfg pull_max_messages=3` | 3 |
| `-target pull_max_messages=4` | 4 |
| `-cfg pull_max_messages=3 -set pull_max_messages=5` | 5 |
| `-set pull_max_bytes=1000` | 7 or 8, as many messages as fit in 1000 bytes |
| `-set pull_max_messages=4 -set pull_threshold_messages=4` | 5 |
| `-set pull_expiry=2s -set pull_heartbeat=1s` | 20 |

The runtime configuration's value applies when the subscriber sets none, and
the subscriber's own value outranks it. With the threshold equal to the
batch, the subscriber pulls again as soon as the first message is handed
off, so it takes one more. Every `serve` exits on Ctrl-C after its 1s drain.

nats.go checks the limits of the pull settings when `Start` begins pulling:

```sh
go run ./docs/runbooks/cmd/serve -group pull-bad -set stream=DEMO_CREATED -set pull_expiry=500ms
go run ./docs/runbooks/cmd/serve -group pull-bad -set stream=DEMO_CREATED -set pull_heartbeat=20s
```

They exit with
`Start: nats: invalid jetstream option: nats: invalid jetstream option: expires value must be at least 1s`
and
`Start: nats: invalid jetstream option: nats: invalid jetstream option: the value of Heartbeat must be less than 50% of expiry`.

## Stopping

`serve` starts first, so its consumer exists before the publish. With
`deliver_policy=new`, a consumer created after the publish never sees it. The
message ID differs on every run, because the stream drops a repeated
`Mesh-Message-Id` within its two-minute duplicate window without an error.

```sh
# terminal 2
go run ./docs/runbooks/cmd/serve -group drain2 -set stream=DEMO_CREATED -set deliver_policy=new -work 30s -drain 1s
# terminal 3, once terminal 2 prints "serving"
go run ./docs/runbooks/cmd/send -set stream=DEMO_CREATED -md Mesh-Message-Id=drain-$(date +%s)
# terminal 2, within 30s of "call 1", while the handler runs: Ctrl-C
```

The drain ends before the handler, so `Stop` cancels the handler's context and
naks the message:

```
stopping with a 1s drain
call 1: context done: context canceled
WARN nats: nacked a message at the end of the drain subject=demo.created stream=DEMO_CREATED consumer=drain2 stream_sequence=1 message_id=drain-1762215260 handler_running=true
Stop: context deadline exceeded
```

When terminal 2 prints `call 1` and then only `stopped` on Ctrl-C, the
handler had already finished. When it prints no `call 1` at all, the publish
was a duplicate or came before the consumer existed.

Running the same `serve` command again delivers the message once more, with
`Mesh-Delivery-Attempt: 2` and the same `Mesh-Message-Id`.

## Cleaning Up

```sh
go run ./docs/runbooks/cmd/streams rm DEMO_CREATED
```

Stopping `nats-server` with Ctrl-C ends everything else. The next session's
`mktemp -d` starts with an empty store.
