package wal

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSnapshot_Load(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "load_test.bin")
	file, err := os.Create(filePath)

	if err != nil {
		t.Errorf("Failed to create test file: %v", err)
		return
	}

	defer file.Close()

	expectedData := make(map[string]string)
	expectedData["user:123"] = "value"
	expectedData["user:456"] = "value2"

	err = gob.NewEncoder(file).Encode(expectedData)

	if err != nil {
		t.Error("Failed to write a test data")
		return
	}

	snapshoter := NewSnapshoter(filePath)

	actualData, err := snapshoter.Load()

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !reflect.DeepEqual(expectedData, actualData) {
		t.Errorf("Want %v but got %v", expectedData, actualData)
	}
}

func TestSnapshot_Save(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "store_test.bin")

	expectedData := make(map[string]string)
	expectedData["key:1"] = "value123"
	expectedData["key:2"] = "value321"

	snapshoter := NewSnapshoter(filePath)

	err := snapshoter.Save(expectedData)

	if err != nil {
		t.Errorf("error while snapshoter save: %v", err)
	}

	file, err := os.Open(filePath)

	if err != nil {
		t.Errorf("error while reading a snapshot file: %v", err)
	}

	defer file.Close()

	actualData := make(map[string]string)

	if err := gob.NewDecoder(file).Decode(&actualData); err != nil {
		t.Errorf("error while encoding a test snapshot file: %v", err)
	}

	if !reflect.DeepEqual(expectedData, actualData) {
		t.Errorf("want %v but got %v", expectedData, actualData)
	}
}
