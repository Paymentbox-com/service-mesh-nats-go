# Development

This page is for working on the transport itself. It covers the recipes, the
tests, and releasing a version.

## Recipes

```
mise install
just check      # format check, vet, test, vulnerability scan, lint
```

Tool versions are pinned in `mise.toml`. `just` with no arguments lists the
recipes.

| Recipe | What it does |
|---|---|
| `just build` | Compiles everything. |
| `just test` | Runs the test suite with the race detector. |
| `just examples` | Builds the example programs. |
| `just vet` | Runs `go vet`. |
| `just fmt` | Formats the code in place with `gofmt`. |
| `just tidy` | Reconciles `go.mod` and `go.sum`. |
| `just vuln` | Reports reachable vulnerabilities with `govulncheck`. |
| `just lint` | Reports lint findings with `golangci-lint`. |
| `just check` | Runs the format check, `vet`, `test`, `vuln`, and `lint`, in the order CI runs them. |
| `just release` | Tags the current commit with the version in `VERSION`, pushes the tag, and asks the Go module proxy to fetch it. It refuses a working tree with changes. |

## Tests

The integration tests start an embedded `nats-server` on a random loopback port
for each test, so they need permission to bind a local TCP socket. Nothing else
needs to be running.

## Releasing

A Go version is released by its tag alone. Nothing is built or uploaded.

1. Set the new version in `VERSION`.
2. Commit, push `master`, and wait for CI to pass.
3. Run `just release`.

The proxy fetch in `just release` only makes the new version resolve for others
right away.
