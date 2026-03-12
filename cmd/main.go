package main

import (
	"context"
	"fmt"
	"kv-store/server"
	"kv-store/store"
	"kv-store/wal"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer, err := wal.NewWalWriter(wal.FILE_NAME)

	if err != nil {
		log.Fatalf("Error while creating WAL writer: %v\n", err)
	}

	snapshotter := wal.NewSnapshotter(wal.SNAPSHOT_FILE_NAME)

	s := store.New(writer, snapshotter, &store.Config{Capacity: 100})

	err = s.Load()

	if err != nil {
		log.Fatalf("Error while populating store: %v\n", err)
	}

	s.EnableCleanup()

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

	s.CreateSnapshot()
	s.Close()
	writer.Close()

	fmt.Println("Shutdown complete")

}
