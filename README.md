# KV Store - Key-Value Database in Go

A learning project to build a key-value store database from scratch.

## Project Structure

```
kv-store-go/
├── PROJECT_PLAN.txt    # Complete implementation plan
├── store/              # Core KV store implementation
│   ├── store.go        # Main store logic
│   └── store_test.go   # Unit tests
├── cmd/                # CLI application
│   └── main.go         # Entry point
└── go.mod              # Go module file
```

## Getting Started

See `PROJECT_PLAN.txt` for the complete phased implementation plan.

### Phase 1: Basic In-Memory Store
Start by implementing the core operations in `store/store.go`

### Run Tests
```bash
go test ./store -v
```

### Run Application
```bash
go run cmd/main.go
```
