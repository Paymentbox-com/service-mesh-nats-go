# Task runner for service-mesh-nats-go. Run `just` with no arguments to see the menu.
#
# Every Go recipe runs through `mise exec` so it uses the toolchain pinned in
# mise.toml without relying on the shell's mise activation. If you ever run just
# from a bare environment where `mise` is not on PATH, change this to
# "/opt/homebrew/bin/mise exec -- go".
go := "mise exec -- go"

# List all recipes
default:
    @just --list

# Compile everything
[group('build')]
build:
    {{go}} build ./...

# Run the full test suite with the race detector
[group('build')]
test:
    {{go}} test -race ./...

# Build the example programs
[group('examples')]
examples:
    {{go}} build -o /dev/null ./examples/...

# Run go vet
[group('checks')]
vet:
    {{go}} vet ./...

# Format the code in place
[group('checks')]
fmt:
    gofmt -w .

# Fail if any file is not gofmt-formatted
[private]
[group('checks')]
fmt-check:
    test -z "$(gofmt -l .)"

# Reconcile go.mod and go.sum
[group('checks')]
tidy:
    {{go}} mod tidy

# Report reachable vulnerabilities (matches CI)
[group('checks')]
vuln:
    {{go}} run golang.org/x/vuln/cmd/govulncheck@latest ./...

# There is no .golangci.yml, so lint runs golangci-lint's default linters. CI
# installs golangci-lint through its GitHub action rather than with `go run`,
# so the two can differ by a release.

# Report lint findings (matches CI)
[group('checks')]
lint:
    {{go}} run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run ./...

# Everything the pre-push gate checks, in the order CI runs them
[group('checks')]
check: fmt-check vet test vuln lint
