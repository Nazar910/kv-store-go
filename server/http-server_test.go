package server

import (
	"bytes"
	"encoding/json"
	"iter"
	"kv-store/store"
	"kv-store/types"
	"kv-store/wal"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
}

func assertEqual[T any](t *testing.T, got T, want T) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}

	t.Errorf("got: %v, want: %v", got, want)
}

type wallMock struct{}

func (w wallMock) Append(cmd wal.Command)            {}
func (w wallMock) CommandSeq() iter.Seq[wal.Command] { return func(yield func(wal.Command) bool) {} }
func (w wallMock) Truncate() error                   { return nil }

type mockSnapshotter struct{}

func (s *mockSnapshotter) Save(types.StoreMap) error     { return nil }
func (s *mockSnapshotter) Load() (types.StoreMap, error) { return nil, nil }

var storeConfig *store.Config = &store.Config{
	Capacity: 100,
}

func setupApp() (*httptest.Server, *store.Store) {
	store := store.New(&wallMock{}, &mockSnapshotter{}, storeConfig)
	originalServer := NewServer(store)
	originalServer.Init()
	server := httptest.NewServer(originalServer.mutex)

	return server, store
}

func makeRequest(t *testing.T, url string, buff bytes.Buffer) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, &buff)
	assertNoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	assertNoError(t, err)

	assertEqual(t, resp.StatusCode, 200)

	return resp
}

func TestHttpServer(t *testing.T) {

	t.Run("set", func(t *testing.T) {
		server, store := setupApp()
		defer server.Close()
		setReq := &SetReq{Key: "foo", Value: "bar123"}

		var buff bytes.Buffer
		err := json.NewEncoder(&buff).Encode(setReq)
		assertNoError(t, err)

		makeRequest(t, server.URL+"/set", buff)

		storedValue, _ := store.Get("foo")
		assertEqual(t, storedValue, "bar123")
	})

	t.Run("setex", func(t *testing.T) {
		server, store := setupApp()
		defer server.Close()
		setReq := &SetReq{Key: "foo", Value: "bar123", Ttl: 30}

		var buff bytes.Buffer
		err := json.NewEncoder(&buff).Encode(setReq)
		assertNoError(t, err)

		makeRequest(t, server.URL+"/setex", buff)

		storedValue, _ := store.Get("foo")
		assertEqual(t, storedValue, "bar123")
	})

	t.Run("get", func(t *testing.T) {
		server, store := setupApp()
		defer server.Close()
		store.Set("foo", "bar321")

		getReq := &GetReq{Key: "foo"}
		var buff bytes.Buffer
		err := json.NewEncoder(&buff).Encode(getReq)
		assertNoError(t, err)

		resp := makeRequest(t, server.URL+"/get", buff)
		var getRes GetRes
		err = json.NewDecoder(resp.Body).Decode(&getRes)
		assertNoError(t, err)
		assertEqual(t, getRes, GetRes{Value: "bar321"})
	})

	t.Run("exists", func(t *testing.T) {
		server, store := setupApp()
		defer server.Close()
		store.Set("foo-exists", "bar")

		getReq := &GetReq{Key: "foo-exists"}
		var buff bytes.Buffer
		err := json.NewEncoder(&buff).Encode(getReq)
		assertNoError(t, err)

		resp := makeRequest(t, server.URL+"/exists", buff)
		var existsRes ExistsRes
		err = json.NewDecoder(resp.Body).Decode(&existsRes)
		assertNoError(t, err)
		assertEqual(t, existsRes, ExistsRes{Exists: true})
	})

	t.Run("delete", func(t *testing.T) {
		server, store := setupApp()
		defer server.Close()
		store.Set("foo-to-delete", "bar")

		getReq := &GetReq{Key: "foo-to-delete"}
		var buff bytes.Buffer
		err := json.NewEncoder(&buff).Encode(getReq)
		assertNoError(t, err)

		makeRequest(t, server.URL+"/delete", buff)
		exists := store.Exists("foo-to-delete")
		assertEqual(t, exists, false)
	})

}
