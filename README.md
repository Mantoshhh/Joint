# Joint

A Redis-backed distributed lock for Go, implementing the Redlock algorithm with fencing tokens.

Joint is an embedded client library, not a standalone lock service — each process that needs the lock imports the package and talks to Redis directly, avoiding an extra network hop through a middleman.

## Design

- **Lock primitive**: `SET key value NX PX ttl` against each Redis instance, with a Lua script (`GET` + conditional `DEL`) for safe release, so a client can never delete a lock it no longer owns.
- **Replication**: true Redlock — 5 independent Redis masters, no replication between them. Acquiring the lock requires a majority (3 of 5), computed within a timing budget derived from the lock's TTL, so a slow or partially unreachable quorum can't succeed after the lock would already be considered expired. A failed acquisition releases whatever it managed to acquire on all 5 instances.
- **Fencing tokens**: every successful `Acquire` returns a monotonically increasing token (a nanosecond timestamp), which a protected resource can use to reject writes from a client that held the lock, stalled past its TTL, and resumed after someone else took over. Enforcing this is the caller's responsibility — the lock only supplies the token.

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
	// ... do work protected by the lock, passing `token` to anything
	// that needs to reject stale writes from an expired holder ...
}
```

## Development

The test suite runs against 5 independent local Redis instances. Bring them up with:

```sh
docker compose up -d
```

This starts 5 containers (`redis:7-alpine`) on ports `6379`-`6383`, matching the 5 addresses the tests expect. Then run:

```sh
go test ./... -race
```

## License

BSD 3-Clause, see [LICENSE](LICENSE).
