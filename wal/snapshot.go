package wal

import (
	"encoding/gob"
	"fmt"
	"kv-store/types"
	"os"
)

type Snapshotter interface {
	Save(types.StoreMap) error
	Load() (types.StoreMap, error)
}

type BinFileSnapshotter struct {
	filePath string
}

const SNAPSHOT_FILE_NAME = "data/snapshot.bin"

func NewSnapshotter(filePath string) Snapshotter {
	return &BinFileSnapshotter{
		filePath: filePath,
	}
}

func (s *BinFileSnapshotter) Save(data types.StoreMap) error {
	tmpPath := s.filePath + ".tmp"
	file, err := os.Create(tmpPath)

	if err != nil {
		return fmt.Errorf("failed creating snapshot file: %v", err)
	}
	defer file.Close()

	if err := gob.NewEncoder(file).Encode(data); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed while writing to a snapshot: %v", err)
	}

	if err := file.Sync(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed while syncing file to a disk: %v", err)
	}

	return os.Rename(tmpPath, s.filePath)
}

func (s *BinFileSnapshotter) Load() (types.StoreMap, error) {
	file, err := os.Open(s.filePath)

	if err != nil {
		if os.IsNotExist(err) {
			return make(types.StoreMap), nil
		}

		return nil, fmt.Errorf("failed to open a snapshot file: %v", err)
	}

	defer file.Close()

	data := make(types.StoreMap)

	if err := gob.NewDecoder(file).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed while reading a snapshot file: %v", err)
	}

	return data, nil
}
