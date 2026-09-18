# End-to-end walkthrough

Two Go processes, a server and a client, talking through a local NATS
broker. Every step lists the output it produces.

Open three terminals in this directory.

## Terminal 1, the broker

```
nats-server
```

It listens on `127.0.0.1:4222`, which is the default both programs use. Set
`NATS_URL` on any command below to point at a different address.

## Terminal 2, the server process

```
INSTANCE=alpha go run ./examples/server
```

It logs `serving demo.echo and demo.created; Ctrl-C to stop` and then prints
every request and event it receives.

## Terminal 3, the client process

Each command is a fresh process with its own NATS connection.

### 1. Request/response round trip with metadata in both directions

```
go run ./examples/client request hello
```

Client prints `reply after 1ms: "echo: hello" metadata map[Echoed-By:alpha]`.
Terminal 2 logs the request with the client's `Sent-At` header.

### 2. Fire-and-forget publish to the subscriber

```
go run ./examples/client publish "order 42"
```

Client prints `published`. Terminal 2 logs `created event "order 42"`.

### 3. Handler error surfaced to the caller

```
go run ./examples/client request fail
```

Client prints `handler failed on the server: invalid argument`. Terminal 2
logs the error at ERROR level.

### 4. Runtime request timeout

```
TIMEOUT=1s go run ./examples/client request slow
```

Client prints `request timed out` after one second. The server handler keeps
running to five seconds; its reply goes nowhere.

### 5. Contract misuse is caught before the wire

```
go run ./examples/echo
```

The last line is
`publish to a route target: mesh: target kind does not match its use`.

### 6. One deployment, two instances

In a fourth terminal start a second instance of the same deployment:

```
INSTANCE=beta go run ./examples/server
```

Then in terminal 3 run the request and the publish several times.

```
go run ./examples/client request hello
go run ./examples/client publish "order 43"
```

`Echoed-By` alternates between `alpha` and `beta`, and each event shows up
in only one server's log. Both instances share the deployment group `demo`.

### 6b. Two deployments

Stop `beta` and start it again as a different deployment:

```
DEPLOYMENT=audit INSTANCE=beta go run ./examples/server
```

Publish again. The event now shows up in both logs, once per deployment.
Requests still go to one instance, but which one is now arbitrary since
both deployments serve `demo.echo`.

### 7. Graceful drain

In terminal 3 start a slow request, then within a few seconds press Ctrl-C in
terminal 2.

```
go run ./examples/client request slow
```

Terminal 2 logs `stopping, draining up to 10s`, waits for the handler, then
`stopped`. The client still receives `reply after 5.0s: "echo: slow"`.

### 8. No responders

With every server stopped:

```
go run ./examples/client request hello
```

Client prints `no server is serving demo.echo` immediately.
