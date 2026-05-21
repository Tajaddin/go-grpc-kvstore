.PHONY: proto test cover run load build docker tidy

# Regenerate Go code from proto (needs protoc + protoc-gen-go + protoc-gen-go-grpc)
proto:
	protoc --go_out=. --go_opt=module=github.com/Tajaddin/go-grpc-kvstore \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/Tajaddin/go-grpc-kvstore \
	       proto/kv.proto

tidy:
	go mod tidy

test:
	go test ./... -race

cover:
	go test ./internal/... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

run:
	go run ./cmd/server

# Run after `make run` in another shell.
load:
	go run ./load -addr localhost:50051 -workers 64 -requests 200000

build:
	go build -o bin/server ./cmd/server

docker:
	docker build -t go-grpc-kvstore:latest .
