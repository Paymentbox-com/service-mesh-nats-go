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
# installs golangci-lint through its GitHub action and this recipe uses
# `go run`, so the two can differ by a release.

# Report lint findings (matches CI)
[group('checks')]
lint:
    {{go}} run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run ./...

# Everything the pre-push gate checks, in the order CI runs them
[group('checks')]
check: fmt-check vet test vuln lint

# The version in VERSION, which names the release tag
version := `cat VERSION`

# Refuses a working tree with changes and a VERSION that is not vX.Y.Z. The
# proxy fetch makes the new version resolve for others right away.
#
# Tag the current commit with the version in VERSION, push the tag, and have the Go proxy fetch it
[group('release')]
release:
    echo "{{version}}" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$' || (echo "VERSION must look like v1.2.3" && exit 1)
    test -z "$(git status --porcelain)" || (echo "commit or stash your changes first" && exit 1)
    git tag -a {{version}} -m "{{version}}"
    git push origin {{version}}
    GOPROXY=https://proxy.golang.org GOFLAGS=-mod=mod {{go}} list -m github.com/Paymentbox-com/service-mesh-nats-go@{{version}}

# Bump the version in VERSION by one patch, minor, or major step: just bump patch
[group('release')]
bump part:
    #!/usr/bin/env bash
    set -euo pipefail
    current="$(cat VERSION)"
    current="${current#v}"
    IFS=. read -r major minor patch <<< "$current"
    case "{{part}}" in
      patch) patch=$((patch + 1)) ;;
      minor) minor=$((minor + 1)); patch=0 ;;
      major) major=$((major + 1)); minor=0; patch=0 ;;
      *) echo "part must be patch, minor, or major" >&2; exit 1 ;;
    esac
    next="${major}.${minor}.${patch}"
    echo "v$next" > VERSION
    echo "v$current -> v$next"
