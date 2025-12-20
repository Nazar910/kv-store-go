package store

import (
	"container/list"
	"fmt"
	"kv-store/types"
	"kv-store/wal"
	"sync"
	"time"
)

type Config struct {
	Capacity int
}

type Clock interface {
	Now() time.Time
}

// Store represents an in-memory key-value store
type Store struct {
	memoryStore types.StoreMap
	mutex       sync.RWMutex
	capacity    int
	lruList     *list.List
	lruMap      map[string]*list.Element

	walWriter   wal.WalManager
	snapshotter wal.Snapshotter
	clock       Clock
}

// New creates a new Store instance
func New(clock Clock, walWriter wal.WalManager, snapshotter wal.Snapshotter, config *Config) *Store {
	return &Store{
		memoryStore: make(types.StoreMap),
		walWriter:   walWriter,
		snapshotter: snapshotter,
		clock:       clock,

		lruList:  list.New(),
		lruMap:   make(map[string]*list.Element),
		capacity: config.Capacity,
	}
}

func (s *Store) AtCapacity() bool {
	return s.lruList.Len() >= s.capacity
}

// Set stores a key-value pair
func (s *Store) Set(key, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.baseSet(key, value, time.Time{}) // zero value means no expiry
}

// Set stores a key-value pair
func (s *Store) SetEx(key, value string, ttl int) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.baseSet(key, value, s.clock.Now().Add(time.Duration(ttl)*time.Second))
}

// Private set implementation (the callee should handle locks itself)
func (s *Store) baseSet(key string, value string, expiresAt time.Time) error {
	s.walWriter.Append(wal.NewSetCommand(key, value))
	_, exists := s.memoryStore[key]

	if !exists && s.AtCapacity() {
		oldest := s.lruList.Back()
		oldestKey := oldest.Value.(string)

		// possible candidate to some privateDelete method
		s.walWriter.Append(wal.NewDeleteCommand(key))
		delete(s.memoryStore, oldestKey)

		delete(s.lruMap, oldestKey)
		s.lruList.Remove(oldest)
	}

	s.memoryStore[key] = &types.Entry{
		Value:     value,
		ExpiresAt: expiresAt,
	}
	if !exists {
		elem := s.lruList.PushFront(key)
		s.lruMap[key] = elem
	} else {
		elem := s.lruMap[key]
		s.lruList.MoveToFront(elem)
	}

	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	result, ok := s.memoryStore[key]

	if !ok {
		return "", nil
	}

	if result.IsExpired(s.clock.Now()) {
		// will need to delete the expired key
		return "", nil
	}

	elem := s.lruMap[key]
	s.lruList.MoveToFront(elem)

	return result.Value, nil
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.walWriter.Append(wal.NewDeleteCommand(key))

	_, exists := s.memoryStore[key]

	if !exists {
		return nil
	}

	node := s.lruMap[key]
	s.lruList.Remove(node)
	delete(s.lruMap, key)
	delete(s.memoryStore, key)

	return nil
}

// Exists checks if a key exists
func (s *Store) Exists(key string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	entry, ok := s.memoryStore[key]
	return ok && !entry.IsExpired(s.clock.Now())
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
			s.memoryStore[cmd.Key] = &types.Entry{
				Value: cmd.Value,
				// setting a default ttl of 30 sec upon WALL restore
				ExpiresAt: s.clock.Now().Add(30 * time.Second),
			}
		case wal.OpDELETE:
			delete(s.memoryStore, cmd.Key)
		}
	})
}
