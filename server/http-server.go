package server

import (
	"context"
	"encoding/json"
	"fmt"
	"kv-store/store"
	"net/http"
)

type Server struct {
	store      *store.Store
	mutex      *http.ServeMux
	httpServer *http.Server
}

func NewServer(store *store.Store) *Server {
	return &Server{
		store: store,
		mutex: http.NewServeMux(),
	}
}

type SetReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Ttl   int    `json:"ttl"`
}

type GetReq struct {
	Key string `json:"key"`
}

type GetRes struct {
	Value string `json:"value"`
}

type ExistsRes struct {
	Exists bool `json:"exists"`
}

func (s *Server) Init() {
	s.mutex.HandleFunc("POST /set", func(w http.ResponseWriter, r *http.Request) {
		var setReq SetReq
		if err := json.NewDecoder(r.Body).Decode(&setReq); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		s.store.Set(setReq.Key, setReq.Value)
		_, err := w.Write([]byte("OK"))

		if err != nil {
			fmt.Printf("Failed to write to response because of err: %v\n", err)
		}
	})
	s.mutex.HandleFunc("POST /setex", func(w http.ResponseWriter, r *http.Request) {
		var setexReq SetReq
		if err := json.NewDecoder(r.Body).Decode(&setexReq); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		s.store.SetEx(setexReq.Key, setexReq.Value, setexReq.Ttl)
		_, err := w.Write([]byte("OK"))

		if err != nil {
			fmt.Printf("Failed to write to response because of err: %v\n", err)
		}
	})
	s.mutex.HandleFunc("POST /get", func(w http.ResponseWriter, r *http.Request) {
		var getReq GetReq
		if err := json.NewDecoder(r.Body).Decode(&getReq); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		value, _ := s.store.Get(getReq.Key)
		w.Header().Set("Content-Type", "application/json")

		res := &GetRes{Value: value}
		err := json.NewEncoder(w).Encode(res)

		if err != nil {
			fmt.Printf("Failed to write the response because of err: %v\n", err)
		}
	})
	s.mutex.HandleFunc("POST /exists", func(w http.ResponseWriter, r *http.Request) {
		var getReq GetReq
		if err := json.NewDecoder(r.Body).Decode(&getReq); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		value := s.store.Exists(getReq.Key)
		w.Header().Set("Content-Type", "application/json")
		res := &ExistsRes{Exists: value}
		err := json.NewEncoder(w).Encode(res)

		if err != nil {
			fmt.Printf("Failed to write the response because of err: %v\n", err)
		}
	})
	s.mutex.HandleFunc("POST /delete", func(w http.ResponseWriter, r *http.Request) {
		var getReq GetReq
		if err := json.NewDecoder(r.Body).Decode(&getReq); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		s.store.Delete(getReq.Key)
		_, err := w.Write([]byte("OK"))

		if err != nil {
			fmt.Printf("Failed to write the response because of err: %v\n", err)
		}
	})
}

func (s *Server) Start(port int) error {
	fmt.Println("Server starting on port", port)
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: s.mutex,
	}

	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}

	return s.httpServer.Shutdown(ctx)
}
