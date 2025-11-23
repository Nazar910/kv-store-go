package main

import (
	"kv-store/server"
	"kv-store/store"
)

func main() {
	s := store.New()
	httpServer := server.NewServer(s)

	httpServer.Init()
	httpServer.Start(3001)
}
