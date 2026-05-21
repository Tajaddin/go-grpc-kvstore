// Package grpcserver adapts the in-memory store to the KvStore gRPC service.
package grpcserver

import (
	"context"
	"time"

	"github.com/Tajaddin/go-grpc-kvstore/gen/kvpb"
	"github.com/Tajaddin/go-grpc-kvstore/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements kvpb.KvStoreServer backed by a store.Store.
type Server struct {
	kvpb.UnimplementedKvStoreServer
	store *store.Store
}

func New(s *store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	value, found := s.store.Get(req.GetKey())
	return &kvpb.GetResponse{Found: found, Value: value}, nil
}

func (s *Server) Set(_ context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	ttl := time.Duration(req.GetTtlMs()) * time.Millisecond
	replaced := s.store.Set(req.GetKey(), req.GetValue(), ttl)
	return &kvpb.SetResponse{Replaced: replaced}, nil
}

func (s *Server) Delete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	existed := s.store.Delete(req.GetKey())
	return &kvpb.DeleteResponse{Existed: existed}, nil
}

func (s *Server) List(_ context.Context, req *kvpb.ListRequest) (*kvpb.ListResponse, error) {
	return &kvpb.ListResponse{Keys: s.store.List(req.GetPrefix())}, nil
}

func (s *Server) Watch(req *kvpb.WatchRequest, stream kvpb.KvStore_WatchServer) error {
	events, cancel := s.store.Watch(req.GetPrefix(), 64)
	defer cancel()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := stream.Send(&kvpb.WatchEvent{
				Type:  toProtoType(ev.Type),
				Key:   ev.Key,
				Value: ev.Value,
			}); err != nil {
				return err
			}
		}
	}
}

func toProtoType(t store.EventType) kvpb.EventType {
	switch t {
	case store.EventSet:
		return kvpb.EventType_EVENT_TYPE_SET
	case store.EventDelete:
		return kvpb.EventType_EVENT_TYPE_DELETE
	case store.EventExpire:
		return kvpb.EventType_EVENT_TYPE_EXPIRE
	default:
		return kvpb.EventType_EVENT_TYPE_UNSPECIFIED
	}
}
