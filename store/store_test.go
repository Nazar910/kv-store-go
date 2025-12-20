package store

import (
	"bytes"
	"fmt"
	"kv-store/wal"
	"reflect"
	"testing"
)

type equaler[T any] interface {
	Equal(T) bool
}

func areEqual[T any](a, b T) bool {

	if isNill(a) && isNill(b) {
		return true
	}

	if aBytes, ok := any(a).([]byte); ok {
		bBytes := any(b).([]byte)
		return bytes.Equal(aBytes, bBytes)
	}

	if eq, ok := any(a).(equaler[T]); ok {
		return eq.Equal(b)
	}

	return reflect.DeepEqual(a, b)
}

func isNill(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)

	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.UnsafePointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func AssertEqual[T any](tb testing.TB, got T, want T) {
	tb.Helper()

	if areEqual(got, want) {
		return
	}

	tb.Errorf("got: %v, want: %v", got, want)
}

type mockWalManager struct{}

func (m *mockWalManager) Append(wal.Command)             {}
func (m *mockWalManager) Replay(func(wal.Command)) error { return nil }
func (m *mockWalManager) Truncate() error                { return nil }

type mockSnapshotter struct{}

func (s *mockSnapshotter) Save(map[string]string) error     { return nil }
func (s *mockSnapshotter) Load() (map[string]string, error) { return nil, nil }

var config *Config = &Config{
	Capacity: 100,
}

func TestStore(t *testing.T) {
	t.Run("set-get", func(t *testing.T) {
		store := New(&mockWalManager{}, &mockSnapshotter{}, config)

		store.Set("foo", "bar")

		got, _ := store.Get("foo")

		AssertEqual(t, got, "bar")
	})

	t.Run("get for nothing should return nil", func(t *testing.T) {
		store := New(&mockWalManager{}, &mockSnapshotter{}, config)

		got, _ := store.Get("foo")

		AssertEqual(t, got, "")
	})

	t.Run("set-delete-get", func(t *testing.T) {
		store := New(&mockWalManager{}, &mockSnapshotter{}, config)

		store.Set("foo", "bar")
		store.Delete("foo")
		got, _ := store.Get("foo")

		AssertEqual(t, got, "")
	})

	t.Run("set-exists", func(t *testing.T) {
		store := New(&mockWalManager{}, &mockSnapshotter{}, config)

		store.Set("foo", "bar")
		got := store.Exists("foo")

		AssertEqual(t, got, true)
	})

	t.Run("lru evicts least used", func(t *testing.T) {
		lruConfig := &Config{Capacity: 5}
		store := New(&mockWalManager{}, &mockSnapshotter{}, lruConfig)

		for v := range 5 {
			key := fmt.Sprintf("foo-%d", v)
			value := fmt.Sprintf("bar-%d", v)
			store.Set(key, value)
		}

		store.Get("foo-0")
		store.Set("foo-5", "bar-5")

		firstExists := store.Exists("foo-0")
		secondExists := store.Exists("foo-1")
		lastExists := store.Exists("foo-5")

		AssertEqual(t, firstExists, true)
		AssertEqual(t, secondExists, false)
		AssertEqual(t, lastExists, true)
	})

}
