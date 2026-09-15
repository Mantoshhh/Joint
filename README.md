# Joint

[![CI](https://github.com/Mantoshhh/Joint/actions/workflows/ci.yaml/badge.svg)](https://github.com/Mantoshhh/Joint/actions/workflows/ci.yaml)
[![golangci-lint](https://github.com/Mantoshhh/Joint/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/Mantoshhh/Joint/actions/workflows/golangci-lint.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Mantoshhh/Joint.svg)](https://pkg.go.dev/github.com/Mantoshhh/Joint)
[![Go Report Card](https://goreportcard.com/badge/github.com/Mantoshhh/Joint)](https://goreportcard.com/report/github.com/Mantoshhh/Joint)
[![License](https://img.shields.io/github/license/Mantoshhh/Joint)](LICENSE)

A Redis-backed distributed lock for Go, implementing the Redlock algorithm **with fencing tokens**, which the most widely-used Go Redlock library does not provide.

Joint is an embedded client library, not a standalone lock service. Each process that needs the lock imports the package and talks to Redis directly, avoiding an extra network hop through a middleman.

## Why fencing tokens matter

Redlock's mutual exclusion alone doesn't protect a resource from a client that acquired the lock, stalled (GC pause, slow network, de-scheduled process) past its TTL, and resumed after another client already took over. Both clients can end up believing they hold the lock at once. Martin Kleppmann's well-known critique of Redlock ("[How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)") identifies fencing tokens as the actual fix: a monotonically increasing number, handed out on every successful acquisition, that the *protected resource* checks and uses to reject any write carrying a token older than one it's already seen.

## Design

- **Lock primitive**: `SET key value NX PX ttl` against each Redis instance, with a Lua script (`GET` + conditional `DEL`) for safe release, so a client can never delete a lock it no longer owns.
- **Replication**: true Redlock, 5 independent Redis masters, no replication between them. Acquiring the lock requires a majority (3 of 5), computed within a timing budget derived from the lock's TTL, so a slow or partially unreachable quorum can't succeed after the lock would already be considered expired. A failed acquisition releases whatever it managed to acquire on all 5 instances.
- **Fencing tokens**: every successful `Acquire` returns a monotonically increasing token (a nanosecond timestamp), which a protected resource can use to reject writes from a client that held the lock, stalled past its TTL, and resumed after someone else took over. Enforcing this is the caller's responsibility, the lock only supplies the token.

## Install

```sh
go get github.com/Mantoshhh/Joint
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"time"

	joint "github.com/Mantoshhh/Joint"
)

func main() {
	addrs := []string{
		"localhost:6379",
		"localhost:6380",
		"localhost:6381",
		"localhost:6382",
		"localhost:6383",
	}

	lock := joint.New(addrs, "my-resource", 5*time.Second)

	acquired, token, err := lock.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	if !acquired {
		fmt.Println("could not acquire lock")
		return
	}
	defer lock.Release(context.Background())

	fmt.Printf("acquired lock, fencing token: %d\n", token)
	// Pass `token` to the resource this lock protects. It should reject
	// any write carrying a token lower than the last one it has seen —
	// that's what actually stops a stalled, expired holder from
	// clobbering state after another client has taken over the lock.
}
```

## Development

Joint has two tiers of tests:

- **Unit tests** — fast, no external dependencies, run against a fake Redis client. Just run:
  ```sh
  go test ./... -race
  ```
- **Integration tests** — run against 5 real, independent Redis instances, exercising the actual quorum/replication behavior end to end. Bring them up with:
  ```sh
  docker compose up -d
  ```
  This starts 5 containers (`redis:7-alpine`) on ports `6379`-`6383`. Then run:
  ```sh
  go test ./... -race -tags=integration
  ```
  (this also runs the unit tests alongside them). The addresses default to match `docker-compose.yml`; override them with the `JOINT_TEST_REDIS_ADDRS` environment variable (comma-separated) if you're running Redis elsewhere.

Lint locally with [`golangci-lint`](https://golangci-lint.run/) (config in `.golangci.yml`, same as CI):
```sh
golangci-lint run ./... --build-tags=integration
```

## License

BSD 3-Clause, see [LICENSE](LICENSE).
