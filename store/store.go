package store

import (
	"fmt"
	"kv-store/wal"
	"sync"
)

// Store represents an in-memory key-value store
type Store struct {
	memoryStore map[string]string
	mutex       sync.RWMutex
	walWriter   wal.WalManager
}

// New creates a new Store instance
func New(walWriter wal.WalManager) *Store {
	return &Store{
		memoryStore: make(map[string]string),
		walWriter:   walWriter,
	}
}

// Set stores a key-value pair
func (s *Store) Set(key string, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.walWriter.Append(wal.NewSetCommand(key, value))

	s.memoryStore[key] = value
	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	result, ok := s.memoryStore[key]

	if !ok {
		return "", nil
	}

	return result, nil
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.walWriter.Append(wal.NewDeleteCommand(key))

	delete(s.memoryStore, key)
	return nil
}

// Exists checks if a key exists
func (s *Store) Exists(key string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, ok := s.memoryStore[key]
	return ok
}

func (s *Store) PopulateFromWal() error {
	fmt.Println("Recovering from WAL...")

	return s.walWriter.Replay(func(cmd wal.Command) {
		switch cmd.Op {
		case wal.OpSET:
			s.memoryStore[cmd.Key] = cmd.Value
		case wal.OpDELETE:
			delete(s.memoryStore, cmd.Key)
		}
	})

}
