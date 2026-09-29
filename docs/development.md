# Development


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
