package grpcserver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Tajaddin/go-grpc-kvstore/gen/kvpb"
	"github.com/Tajaddin/go-grpc-kvstore/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newTestClient spins up the gRPC server over an in-memory bufconn listener
// and returns a connected client plus a cleanup func.
func newTestClient(t *testing.T) (kvpb.KvStoreClient, *store.Store, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	st := store.New(0)
	srv := grpc.NewServer()
	kvpb.RegisterKvStoreServer(srv, New(st))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		st.Close()
	}
	return kvpb.NewKvStoreClient(conn), st, cleanup
}

func TestGrpcSetGetDelete(t *testing.T) {
	client, _, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()

	setResp, err := client.Set(ctx, &kvpb.SetRequest{Key: "k", Value: []byte("v")})
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if setResp.GetReplaced() {
		t.Fatal("first set should not be a replace")
	}

	getResp, err := client.Get(ctx, &kvpb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !getResp.GetFound() || string(getResp.GetValue()) != "v" {
		t.Fatalf("get = %+v, want found=true value=v", getResp)
	}

	delResp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: "k"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !delResp.GetExisted() {
		t.Fatal("delete should report existed=true")
	}
}

func TestGrpcGetRejectsEmptyKey(t *testing.T) {
	client, _, cleanup := newTestClient(t)
	defer cleanup()
	_, err := client.Get(context.Background(), &kvpb.GetRequest{Key: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestGrpcList(t *testing.T) {
	client, _, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()
	client.Set(ctx, &kvpb.SetRequest{Key: "a:1", Value: []byte("x")})
	client.Set(ctx, &kvpb.SetRequest{Key: "a:2", Value: []byte("y")})
	client.Set(ctx, &kvpb.SetRequest{Key: "b:1", Value: []byte("z")})

	resp, err := client.List(ctx, &kvpb.ListRequest{Prefix: "a:"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.GetKeys()) != 2 {
		t.Fatalf("list = %v, want 2 keys", resp.GetKeys())
	}
}

func TestGrpcWatchStream(t *testing.T) {
	client, _, cleanup := newTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Watch(ctx, &kvpb.WatchRequest{Prefix: "watched:"})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	// Give the server a moment to register the watcher before writing.
	time.Sleep(100 * time.Millisecond)

	go func() {
		client.Set(context.Background(), &kvpb.SetRequest{Key: "watched:1", Value: []byte("v")})
	}()

	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if ev.GetType() != kvpb.EventType_EVENT_TYPE_SET || ev.GetKey() != "watched:1" {
		t.Fatalf("event = %+v, want Set watched:1", ev)
	}
}
