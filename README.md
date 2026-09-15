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
- **Fencing tokens**: every successful `Acquire` returns a monotonically increasing token, which a protected resource can use to reject writes from a client that held the lock, stalled past its TTL, and resumed after someone else took over. Enforcing this is the caller's responsibility, the lock only supplies the token.

## Fencing token safety

The fencing token is *not* derived from client clocks or from the N Redlock instances. Which subset of those N instances forms quorum varies attempt to attempt, so a counter incremented across "whichever instances quorum happened to touch" has no globally agreed value. Propagating it to all N atomically with the lock grant is itself a consensus problem Redlock is structurally not built to solve (see Martin Kleppmann's critique linked above).

Instead, the token comes from a separate, genuinely consistent store: a single Redis counter (`INCR`), managed by Redis Sentinel for failover, decoupled from the Redlock quorum set. `Acquire` only reads this counter *after* Redlock quorum is won, so a failed acquisition never burns a token, and it waits for at least one replica to acknowledge the increment (`WAIT`) before handing the token back. This closes the window where a promoted replica could be missing the latest increment and hand out a regressed token after failover.

This is a deliberate, explicit tradeoff: lock *availability* still tolerates up to a minority of the N Redlock instances failing, but fencing *safety* additionally depends on the Sentinel-managed counter being reachable. The counter can be unreachable for a few reasons: not configured, unreachable over the network, or `WAIT` can't confirm replication. In any of those cases, `Acquire` fails closed with `ErrNoCounter` or `ErrNoAck` rather than handing out a weaker or duplicate token. Enable it with `WithCounter(masterName, sentinelAddrs)`; without it, `Acquire` always fails.

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
	sentinelAddrs := []string{"localhost:6386", "localhost:6387", "localhost:6388"}

	lock := joint.New(addrs, "my-resource", 5*time.Second,
		joint.WithCounter("mymaster", sentinelAddrs))

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
	// any write carrying a token lower than the last one it has seen.
	// That's what actually stops a stalled, expired holder from
	// clobbering state after another client has taken over the lock.
}
```

## Development

Joint has two tiers of tests:

- **Unit tests**. Fast, no external dependencies, run against a fake Redis client. Just run:
  ```sh
  go test ./... -race
  ```
- **Integration tests**. Run against 5 real, independent Redis instances (the Redlock quorum set) plus a Sentinel-managed counter (a primary, a replica, and 3 sentinels), exercising the actual quorum/replication and fencing-token behavior end to end. Bring the infra up with:
  ```sh
  docker compose up -d
  ```
  This starts the 5 Redlock nodes (`redlock-1..5`, ports `6379`-`6383`) and the counter's primary/replica/sentinels (`counter-primary`, `counter-replica`, `counter-sentinel-1..3`, ports `6384`-`6388`).

  Run the tests **inside the Compose network**, not directly on the host:
  ```sh
  docker compose --profile integration run --rm tests
  ```
  This matters because the sentinels announce the counter's Compose-internal hostname (`counter-primary`) as its address. That only resolves for other containers on the same Compose network, not for a `go test` process running on your host machine. The `tests` service (gated behind the `integration` profile so it never starts on a plain `docker compose up -d`) runs `go test -tags integration ./...` (this also runs the unit tests alongside them) from inside that network. Override the target addresses with the `JOINT_TEST_REDIS_ADDRS`/`JOINT_TEST_SENTINEL_ADDRS` environment variables (comma-separated) if you're running the infra elsewhere; pass extra flags by appending them, e.g. `docker compose --profile integration run --rm tests go test ./... -race -tags=integration`.

Lint locally with [`golangci-lint`](https://golangci-lint.run/) (config in `.golangci.yml`, same as CI):
```sh
golangci-lint run ./... --build-tags=integration
```

## License

BSD 3-Clause, see [LICENSE](LICENSE).
