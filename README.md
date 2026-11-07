# go-grpc-kvstore

> In-memory key-value store served over **gRPC + REST at once**. On the Get path over real gRPC (HTTP/2 + protobuf, 64 workers, 200k/200k OK) the current sharded store measured **~118,000 ops/sec at p99 1.19ms** (2026-11-07, 3-run median; details under [Hero metrics](#hero-metrics)). TTL expiry, a streaming Watch, sharded concurrency, distroless image. 24 tests with the race detector, no external infra.

[![ci](https://github.com/Tajaddin/go-grpc-kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/Tajaddin/go-grpc-kvstore/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26-00ADD8)](go.mod)

## Hero metrics

The hero benchmark:

```bash
go run ./cmd/server &
go run ./load -addr localhost:50051 -workers 64 -requests 200000
```

Measured 2026-11-07 on the current code (sharded store, grpc 1.83.2, commit fd7215f): 3 consecutive runs on an i5-13600KF desktop (Windows 11, Go 1.27.3), CPU load 22% immediately before the first run. The May 2026 figure of ~51,800 ops/sec at p99 2.84 ms was the earlier single-lock version (grpc 1.81.1) on a different machine (i7-10875H laptop), so the two are not a like-for-like comparison. Full output, hardware and machine state in [`bench/results.txt`](bench/results.txt).

| Metric | Value |
|---|---:|
| **Throughput (gRPC Get)** | **~118,000 ops/sec** (3-run median 118,222; max 123,608) |
| Latency p99 | **~1.19 ms** (3-run median) |
| Success rate | 200,000 / 200,000 (100%) per run |

Single server instance, single client process, in-memory store, real gRPC over the loopback (HTTP/2 framing + protobuf marshal on every call).

## What it is

One store, two front doors:

- **gRPC** (`KvStore` service): `Get`, `Set`, `Delete`, `List`, and a server-streaming `Watch`.
- **REST/JSON gateway**: the same operations over plain HTTP for curl and browsers.

| Feature | How |
|---|---|
| Concurrency | 32 shards, each a map behind its own `sync.RWMutex`; keys are placed by `hash/maphash`, so `Get`, `Set` and `Delete` lock only their key's shard (writes also read-lock the watcher list to publish events), and the TTL sweeper locks one shard at a time. Race-detector-clean under 16-goroutine contention tests. |
| TTL expiry | Per-key `expiresAt`; lazy check on read plus a background sweeper that evicts and emits `EXPIRE` events. |
| Watch | Prefix-scoped pub/sub. Events are sent while the key's shard is still locked, so each key's `SET` / `DELETE` / `EXPIRE` events arrive in the order they were applied. Buffered per-watcher channels; a slow consumer drops events instead of blocking writers. |
| Streaming | gRPC server stream wired to the watch channel, terminated cleanly on client cancel. On shutdown the server waits up to 5 s for RPCs to finish, then closes the streams still open. |
| Safety | Stored values are copied in and out, so callers cannot mutate store state through their slices. REST uploads are capped at 1 MiB (`413`), and a body that fails mid-read is rejected (`400`), never stored truncated. |
| Deploy | Multi-stage build to a `distroless/static` nonroot image. |

## Why this matters for hiring

Role categories unlocked: **Backend-Go**, Platform Engineering, Systems, Forward Deployed (infra).

Go + gRPC + load-tested throughput is the exact stack platform and infra teams screen for. This repo backs the "Go / gRPC" resume line with a load-tested service and a reproducible harness.

## How to run

Prerequisites: Go 1.26+ (Docker optional for the container build).

```bash
go test ./...                       # 24 tests (CI runs them with -race)
go run ./cmd/server                 # gRPC on :50051, REST on :8080
go run ./load -addr localhost:50051 -workers 64 -requests 200000   # runs the hero benchmark
docker compose up --build           # alt: distroless container
```

### REST gateway

```bash
curl -X PUT  --data-binary 'hello' 'localhost:8080/kv/greeting?ttl_ms=60000'
curl         'localhost:8080/kv/greeting'      # {"found":true,"value":"aGVsbG8="}  (base64)
curl 'localhost:8080/kv?prefix=gr'             # {"keys":["greeting"]}
curl -X DELETE 'localhost:8080/kv/greeting'    # {"existed":true}
```

### gRPC (grpcurl)

```bash
grpcurl -plaintext -d '{"key":"k","value":"dg=="}' localhost:50051 kv.v1.KvStore/Set
grpcurl -plaintext -d '{"key":"k"}'                localhost:50051 kv.v1.KvStore/Get
grpcurl -plaintext -d '{"prefix":""}'              localhost:50051 kv.v1.KvStore/Watch   # streams
```

## API

gRPC service `kv.v1.KvStore` (see [`proto/kv.proto`](proto/kv.proto)):

| RPC | Type | Purpose |
|---|---|---|
| `Get` | unary | value + found flag |
| `Set` | unary | store with optional `ttl_ms`; reports replaced |
| `Delete` | unary | reports existed |
| `List` | unary | keys by prefix |
| `Watch` | server stream | change events (`SET` / `DELETE` / `EXPIRE`) under a prefix |

## Testing

```bash
go test ./... -race        # 24 tests, race detector on
make cover                 # coverage (store 90.0%, grpc 76.7%, http 80.5%)
```

- **store**: set/get/delete, TTL expiry (injected clock), prefix list, value-copy isolation, watch delivery, slow-consumer drop, watch order under concurrent Sets and under the sweeper, keys spread over all shards, a locked shard not blocking others, 16-goroutine concurrency.
- **grpcserver**: set/get/delete/list over an in-process `bufconn`, empty-key `InvalidArgument`, and a live `Watch` stream round trip.
- **httpgateway**: PUT/GET/DELETE/list/healthz via `httptest`, oversized (413) and truncated (400) uploads not stored.
- **cmd/server**: shutdown closes open `Watch` streams after the grace period and stops at once when idle.

## Project layout

```
proto/kv.proto                  # service + message definitions
gen/kvpb/                       # generated Go (committed, so CI needs no protoc)
internal/store/                 # concurrent map + TTL + watch pub/sub
internal/grpcserver/            # KvStore gRPC implementation
internal/httpgateway/           # REST/JSON gateway over the same store
cmd/server/                     # runs gRPC + HTTP together, graceful shutdown
load/                           # concurrent gRPC load generator
```

## Regenerating proto

The generated code is committed, so a normal build needs no protoc. To regenerate after editing `proto/kv.proto`:

```bash
make proto    # needs protoc + protoc-gen-go + protoc-gen-go-grpc
```

## Stack

Go 1.26, google.golang.org/grpc, protobuf, net/http (REST), distroless Docker, GitHub Actions (vet + race tests + coverage gate + image build).

## License

MIT
