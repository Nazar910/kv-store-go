package server

import (
	"encoding/json"
	"fmt"
	"kv-store/store"
	"net/http"
)

type Server struct {
	store *store.Store
	mutex *http.ServeMux
}

func NewServer(store *store.Store) *Server {
	return &Server{
		store: store,
		mutex: http.NewServeMux(),
	}
}

type SetReq struct {
	Key   string `json:"key" validate:"required,min=1,max=100"`
	Value string `json:"value" validate:"required,min=1,max=200"`
}

type GetReq struct {
	Key string `json:"key" validate:"required,min=1,max=100"`
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

func (s *Server) Start(port int) {
	fmt.Println("Server starting on port", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), s.mutex); err != nil {
		fmt.Printf("Got error while server start up: %v\n", err)
	}
}
