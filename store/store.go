package store

// Store represents an in-memory key-value store
type Store struct {
	memoryStore map[string]string
}

// New creates a new Store instance
func New() *Store {
	return &Store{
		memoryStore: make(map[string]string),
	}
}

// Set stores a key-value pair
func (s *Store) Set(key string, value string) error {
	s.memoryStore[key] = value
	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, error) {
	result, _ := s.memoryStore[key]
	return result, nil
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) error {
	delete(s.memoryStore, key)
	return nil
}

// Exists checks if a key exists
func (s *Store) Exists(key string) bool {
	_, ok := s.memoryStore[key]
	return ok
}
