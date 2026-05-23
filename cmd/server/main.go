// Command server runs the KvStore over gRPC and a REST/JSON gateway at once.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tajaddin/go-grpc-kvstore/gen/kvpb"
	"github.com/Tajaddin/go-grpc-kvstore/internal/grpcserver"
	"github.com/Tajaddin/go-grpc-kvstore/internal/httpgateway"
	"github.com/Tajaddin/go-grpc-kvstore/internal/store"
	"google.golang.org/grpc"
)

func main() {
	grpcAddr := flag.String("grpc", envOr("GRPC_ADDR", ":50051"), "gRPC listen address")
	httpAddr := flag.String("http", envOr("HTTP_ADDR", ":8080"), "HTTP listen address")
	sweep := flag.Duration("sweep", time.Second, "expired-key sweep interval")
	flag.Parse()

	st := store.New(*sweep)
	defer st.Close()

	// gRPC server
	grpcSrv := grpc.NewServer()
	kvpb.RegisterKvStoreServer(grpcSrv, grpcserver.New(st))
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}
	go func() {
		log.Printf("gRPC listening on %s", *grpcAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	// HTTP gateway
	httpSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           httpgateway.New(st).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("HTTP listening on %s", *httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http serve: %v", err)
		}
	}()

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	grpcSrv.GracefulStop()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
