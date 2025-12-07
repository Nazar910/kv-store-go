package wal

import (
	"encoding/gob"
	"fmt"
	"os"
)

type Snapshoter struct {
	filePath string
}

const SNAPSHOT_FILE_NAME = "data/snapshot.bin"

func NewSnapshoter(filePath string) *Snapshoter {
	return &Snapshoter{
		filePath: filePath,
	}
}

func (s *Snapshoter) Save(data map[string]string) error {
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

func (s *Snapshoter) Load() (map[string]string, error) {
	file, err := os.Open(s.filePath)

	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]string), nil
		}

		return nil, fmt.Errorf("failed to open a snapshot file: %v", err)
	}

	defer file.Close()

	data := make(map[string]string)

	if err := gob.NewDecoder(file).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed while reading a snapshot file: %v", err)
	}

	return data, nil
}
