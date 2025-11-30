package main

import (
	"kv-store/server"
	"kv-store/store"
	"kv-store/wal"
)

func main() {
	writer, _ := wal.NewWalWriter("data/log.txt")

	s := store.New(writer)
	httpServer := server.NewServer(s)

	httpServer.Init()
	httpServer.Start(3001)
}
