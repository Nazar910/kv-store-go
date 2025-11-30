package main

import (
	"context"
	"fmt"
	"kv-store/server"
	"kv-store/store"
	"kv-store/wal"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer, err := wal.NewWalWriter()

	if err != nil {
		fmt.Printf("Error while creating WAL writer: %v\n", err)
		os.Exit(1)
	}

	s := store.New(writer)

	err = s.PopulateFromWal()

	if err != nil {
		fmt.Printf("Error while populating from WAL: %v\n", err)
		os.Exit(1)
	}

	httpServer := server.NewServer(s)

	httpServer.Init()

	serverErrors := make(chan error, 1)

	go func() {
		serverErrors <- httpServer.Start(3001)
	}()

	select {
	case err := <-serverErrors:
		fmt.Printf("Got server error: %v", err)
	case <-ctx.Done():
		fmt.Println("Shutdown signal receiver")
	}

	shutfownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("Shutdown gracefully...")

	if err := httpServer.Shutdown(shutfownCtx); err != nil {
		fmt.Printf("Server shutdown error: %v\n", err)
	}

	writer.Close()

	fmt.Println("Shutdown complete")

}
