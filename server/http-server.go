package server

import (
	"encoding/json"
	"fmt"
	"kv-store/store"
	"net/http"
)

type Server struct {
	store *store.Store
}

func NewServer(store *store.Store) *Server {
	return &Server{
		store: store,
	}
}

type SetReq struct {
	Key   string `json:"key" validate:"required,min=1,max=100"`
	Value string `json:"value" validate:"required,min=1,max=200"`
}

type GetReq struct {
	Key string `json:"key" validate:"required,min=1,max=100"`
}

func (s *Server) Start() {
	http.HandleFunc("POST /set", func(w http.ResponseWriter, r *http.Request) {
		var setReq SetReq
		if err := json.NewDecoder(r.Body).Decode(&setReq); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		s.store.Set(setReq.Key, setReq.Value)
		w.Write([]byte("OK"))
	})
	http.HandleFunc("POST /get", func(w http.ResponseWriter, r *http.Request) {
		var getReq GetReq
		if err := json.NewDecoder(r.Body).Decode(&getReq); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		fmt.Printf("In here: %s \n", getReq.Key)
		value, _ := s.store.Get(getReq.Key)
		w.Write([]byte(value))
	})
	http.ListenAndServe(":3001", nil)
}
