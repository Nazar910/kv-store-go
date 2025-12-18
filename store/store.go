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
	snapshotter wal.Snapshotter
}

// New creates a new Store instance
func New(walWriter wal.WalManager, snapshotter wal.Snapshotter) *Store {
	return &Store{
		memoryStore: make(map[string]string),
		walWriter:   walWriter,
		snapshotter: snapshotter,
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

func (s *Store) CreateSnapshot() error {
	fmt.Println("Saving to a snapshot file...")
	s.mutex.Lock()
	defer s.mutex.Unlock()

	err := s.snapshotter.Save(s.memoryStore)

	if err != nil {
		return err
	}

	return s.walWriter.Truncate()
}

func (s *Store) Load() error {
	fmt.Println("Loading from a snapshot file...")
	data, err := s.snapshotter.Load()

	if err != nil {
		return err
	}

	s.memoryStore = data

	fmt.Println("Catching up with WAL if any...")
	return s.walWriter.Replay(func(cmd wal.Command) {
		switch cmd.Op {
		case wal.OpSET:
			s.memoryStore[cmd.Key] = cmd.Value
		case wal.OpDELETE:
			delete(s.memoryStore, cmd.Key)
		}
	})
}
