package store

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

// TestConcurrentAccess demonstrates race conditions in the current implementation
func TestConcurrentAccess(t *testing.T) {
	s := New(&mockWalManager{}, &mockSnapshotter{}, config)

	// Number of concurrent goroutines
	numGoroutines := 100
	numOperations := 100

	var wg sync.WaitGroup

	// Launch multiple goroutines doing concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				s.Set(key, fmt.Sprintf("value-%d-%d", id, j))
			}
		}(i)
	}

	// Launch goroutines doing concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id%10, j%10)
				s.Get(key)
			}
		}(i)
	}

	// Launch goroutines checking existence
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id%10, j%10)
				s.Exists(key)
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrentReadWrite shows classic read-write race
func TestConcurrentReadWrite(t *testing.T) {
	s := New(&mockWalManager{}, &mockSnapshotter{}, config)
	s.Set("shared-key", "initial-value")

	var wg sync.WaitGroup

	// Writer goroutine - continuously updates the same key
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			s.Set("shared-key", fmt.Sprintf("value-%d", i))
		}
	}()

	// Multiple reader goroutines - reading the same key
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				s.Get("shared-key")
			}
		}(i)
	}

	wg.Wait()
}

// TestMapGrowthRace demonstrates race during map growth
func TestMapGrowthRace(t *testing.T) {
	s := New(&mockWalManager{}, &mockSnapshotter{}, config)

	var wg sync.WaitGroup

	// Multiple goroutines adding keys to force map resizing
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Set(fmt.Sprintf("key-%d-%d", id, j), strconv.Itoa(j))
			}
		}(i)
	}

	wg.Wait()
}

// TestDeleteRace shows delete racing with reads
func TestDeleteRace(t *testing.T) {
	s := New(&mockWalManager{}, &mockSnapshotter{}, config)

	// Pre-populate
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("key-%d", i), strconv.Itoa(i))
	}

	var wg sync.WaitGroup

	// Deleters
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Delete(fmt.Sprintf("key-%d", j))
			}
		}(i)
	}

	// Readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Get(fmt.Sprintf("key-%d", j))
			}
		}(i)
	}

	wg.Wait()
}
