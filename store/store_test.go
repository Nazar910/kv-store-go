package store

import (
	"reflect"
	"testing"
)

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

	if isNill(got) && isNill(want) {
		return
	}

	if reflect.DeepEqual(got, want) {
		return
	}

	tb.Errorf("got: %v, want: %v", got, want)
}

func TestStore(t *testing.T) {
	t.Run("set-get", func(t *testing.T) {
		store := New()

		store.Set("foo", "bar")

		got, _ := store.Get("foo")

		AssertEqual(t, got, "bar")
	})

	t.Run("get for nothing should return nil", func(t *testing.T) {
		store := New()

		got, _ := store.Get("foo")

		AssertEqual(t, got, "")
	})

	t.Run("set-delete-get", func(t *testing.T) {
		store := New()

		store.Set("foo", "bar")
		store.Delete("foo")
		got, _ := store.Get("foo")

		AssertEqual(t, got, "")
	})

	t.Run("set-exists", func(t *testing.T) {
		store := New()

		store.Set("foo", "bar")
		got := store.Exists("foo")

		AssertEqual(t, got, true)
	})

}
