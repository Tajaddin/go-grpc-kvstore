package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Tajaddin/go-grpc-kvstore/gen/kvpb"
	"github.com/Tajaddin/go-grpc-kvstore/internal/grpcserver"
	"github.com/Tajaddin/go-grpc-kvstore/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func startServer(t *testing.T) (*grpc.Server, kvpb.KvStoreClient) {
	t.Helper()
	st := store.New(0)
	t.Cleanup(st.Close)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	kvpb.RegisterKvStoreServer(srv, grpcserver.New(st))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return srv, kvpb.NewKvStoreClient(conn)
}

// stopWithin runs stopGRPC with the given grace period and reports how long it
// took, failing the test if it does not return at all.
func stopWithin(t *testing.T, srv *grpc.Server, grace time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	start := time.Now()
	done := make(chan struct{})
	go func() {
		stopGRPC(ctx, srv)
		close(done)
	}()
	select {
	case <-done:
		return time.Since(start)
	case <-time.After(grace + 5*time.Second):
		t.Fatal("shutdown never returned")
		return 0
	}
}

func TestStopGRPCClosesOpenWatchStreams(t *testing.T) {
	srv, client := startServer(t)
	ctx := context.Background()
	stream, err := client.Watch(ctx, &kvpb.WatchRequest{})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	// Write until an event arrives, which proves the Watch handler is running.
	first := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		first <- err
	}()
	for streaming := false; !streaming; {
		if _, err := client.Set(ctx, &kvpb.SetRequest{Key: "k", Value: []byte("v")}); err != nil {
			t.Fatalf("set: %v", err)
		}
		select {
		case err := <-first:
			if err != nil {
				t.Fatalf("recv: %v", err)
			}
			streaming = true
		case <-time.After(50 * time.Millisecond):
		}
	}

	stopWithin(t, srv, 200*time.Millisecond)
	if _, err := stream.Recv(); err == nil {
		t.Fatal("watch stream still open after shutdown")
	}
}

func TestStopGRPCIsGracefulWhenIdle(t *testing.T) {
	srv, client := startServer(t)
	if _, err := client.Set(context.Background(), &kvpb.SetRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if took := stopWithin(t, srv, 5*time.Second); took > 2*time.Second {
		t.Fatalf("idle shutdown took %v; it should not wait for the grace period", took)
	}
}
