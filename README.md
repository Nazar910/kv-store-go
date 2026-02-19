# KV Store - Key-Value In-Memory Database in Go

A functional key-value store database with persistence, LRU eviction, and TTL support.

## Features

- **In-Memory Storage**: Fast access to data using a thread-safe map.
- **Persistence (WAL)**: Write-Ahead Logging ensures data durability across restarts.
- **Snapshots**: Periodic state snapshots for faster recovery and log truncation.
- **LRU Eviction**: Automatic memory management by evicting Least Recently Used items.
- **TTL Support**: Set expiration times for keys.
- **HTTP API**: Simple RESTful interface for interacting with the store.

## Project Structure

```
kv-store-go/
├── cmd/                # Entry point (Main application)
├── store/              # Core KV store logic (LRU, TTL, State)
├── wal/                # Write-Ahead Log & Snapshotting
├── server/             # HTTP Server & Handlers
├── types/              # Common data structures
├── data/               # Persistent storage files (WAL & Snapshots)
└── PROJECT_PLAN.txt    # Implementation roadmap
```

## API Usage

The server runs on port `3001` by default.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET`  | `/get?key=foo` | Retrieve a value |
| `POST` | `/set` | Store a value (JSON body: `{"key": "foo", "value": "bar"}`) |
| `POST` | `/setex` | Store with TTL (JSON body: `{"key": "foo", "value": "bar", "ttl": 60}`) |
| `DELETE`| `/delete?key=foo` | Remove a key |

## Getting Started

### Run Tests
```bash
go test ./... -v
```

### Run Application
```bash
go run cmd/main.go
```
