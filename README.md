# go-grpc-kvstore

> In-memory key-value store served over **gRPC + REST at once**. **~51,800 ops/sec at p99 2.84ms** on the Get path over real gRPC (HTTP/2 + protobuf, 64 workers, 200k/200k OK). TTL expiry, a streaming Watch, sharded concurrency, distroless image. 16 tests with the race detector, no external infra.

[![ci](https://github.com/Tajaddin/go-grpc-kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/Tajaddin/go-grpc-kvstore/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.24-00ADD8)](go.mod)

## Hero metrics

Reproducible:

```bash
go run ./cmd/server &
go run ./load -addr localhost:50051 -workers 64 -requests 200000
```

Last measured 3-run baseline (full output + hardware in [`bench/results.txt`](bench/results.txt)):

| Metric | Value |
|---|---:|
| **Throughput (gRPC Get)** | **~51,800 ops/sec** (3-run median 51,753; max 51,993) |
| Latency p99 | **~2.84 ms** |
| Success rate | 200,000 / 200,000 (100%) per run |

Single server instance, single client process, in-memory store, real gRPC over the loopback (HTTP/2 framing + protobuf marshal on every call). That is ~5x the "10K QPS" bar most platform-engineering JDs ask for, with a sub-3ms p99.

## What it is

One store, two front doors:

- **gRPC** (`KvStore` service): `Get`, `Set`, `Delete`, `List`, and a server-streaming `Watch`.
- **REST/JSON gateway**: the same operations over plain HTTP for curl and browsers.

| Feature | How |
|---|---|
| Concurrency | `sync.RWMutex`-guarded map; reads take the read lock, writes the write lock. Race-detector-clean under 16-goroutine contention tests. |
| TTL expiry | Per-key `expiresAt`; lazy check on read plus a background sweeper that evicts and emits `EXPIRE` events. |
| Watch | Prefix-scoped pub/sub. Buffered per-watcher channels; a slow consumer drops events instead of blocking writers. |
| Streaming | gRPC server stream wired to the watch channel, terminated cleanly on client cancel. |
| Safety | Stored values are copied in and out, so callers cannot mutate store state through their slices. |
| Deploy | Multi-stage build to a `distroless/static` nonroot image. |

## Why this matters for hiring

Role categories unlocked: **Backend-Go**, Platform Engineering, Systems, Forward Deployed (infra).

Go + gRPC + load-tested throughput is the exact stack platform and infra teams screen for. This repo backs the "Go / gRPC" resume line with a service that actually serves 40K+ ops/sec and proves it with a reproducible harness.

## How to run

Prerequisites: Go 1.23+ (Docker optional for the container build).

```bash
go test ./...                       # 16 unit tests (race detector)
go run ./cmd/server                 # gRPC on :50051, REST on :8080
go run ./load -addr localhost:50051 -workers 64 -requests 200000   # reproduces the hero
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
go test ./... -race        # 16 tests, race detector on
make cover                 # coverage (store 78%, grpc 77%, http 77%)
```

- **store**: set/get/delete, TTL expiry (injected clock), prefix list, value-copy isolation, watch delivery, slow-consumer drop, 16-goroutine concurrency.
- **grpcserver**: set/get/delete/list over an in-process `bufconn`, empty-key `InvalidArgument`, and a live `Watch` stream round trip.
- **httpgateway**: PUT/GET/DELETE/list/healthz via `httptest`.

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

Go 1.24, google.golang.org/grpc, protobuf, net/http (REST), distroless Docker, GitHub Actions (vet + race tests + coverage gate + image build).

## License

MIT
