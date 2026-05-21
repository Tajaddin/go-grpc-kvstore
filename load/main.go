// Command load drives concurrent gRPC traffic against a running KvStore and
// reports throughput (ops/sec) and latency percentiles.
//
//	go run ./cmd/server &                       # start the server
//	go run ./load -addr localhost:50051 -workers 64 -requests 200000
//
// Each request is a Get on a key that was pre-seeded, so the run measures the
// hot read path over real gRPC (HTTP/2 framing, protobuf marshal, the works).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tajaddin/go-grpc-kvstore/gen/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	workers := flag.Int("workers", 64, "concurrent workers")
	requests := flag.Int("requests", 200000, "total requests")
	seed := flag.Int("seed", 1000, "number of keys to pre-seed")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := kvpb.NewKvStoreClient(conn)
	ctx := context.Background()

	// Seed keys.
	for i := 0; i < *seed; i++ {
		_, err := client.Set(ctx, &kvpb.SetRequest{
			Key:   fmt.Sprintf("key:%d", i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			log.Fatalf("seed set: %v", err)
		}
	}

	// Warm up.
	for i := 0; i < 1000; i++ {
		client.Get(ctx, &kvpb.GetRequest{Key: fmt.Sprintf("key:%d", i%*seed)})
	}

	latencies := make([][]time.Duration, *workers)
	var done int64
	perWorker := *requests / *workers

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]time.Duration, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("key:%d", (w*perWorker+i)%*seed)
				t0 := time.Now()
				_, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
				if err != nil {
					continue
				}
				local = append(local, time.Since(t0))
				atomic.AddInt64(&done, 1)
			}
			latencies[w] = local
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	all := make([]time.Duration, 0, *requests)
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	n := len(all)
	qps := float64(n) / elapsed.Seconds()
	fmt.Printf("endpoint:      Get over gRPC (HTTP/2, protobuf)\n")
	fmt.Printf("workers:       %d\n", *workers)
	fmt.Printf("requests_ok:   %d / %d\n", n, *requests)
	fmt.Printf("wall_seconds:  %.3f\n", elapsed.Seconds())
	fmt.Printf("throughput:    %.0f ops/sec\n", qps)
	fmt.Printf("latency_p50:   %s\n", all[int(0.50*float64(n))])
	fmt.Printf("latency_p95:   %s\n", all[int(0.95*float64(n))])
	fmt.Printf("latency_p99:   %s\n", all[int(0.99*float64(n))])
	fmt.Printf("latency_max:   %s\n", all[n-1])
}
