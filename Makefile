.PHONY: test
test:
	go test ./...

.PHONY: lint
lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint run ./...

.PHONY: build
build:
	go build -o kv-store cmd/main.go
