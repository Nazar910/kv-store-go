package wal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWalWriter_AppendAndReplay tests the complete E2E flow of writing and reading from WAL
func TestWalWriter_AppendAndReplay(t *testing.T) {
	// Create isolated temporary directory (automatically cleaned up after test)
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL writer
	writer, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer writer.Close()

	// Append some commands
	commands := []Command{
		NewSetCommand("key1", "value1"),
		NewSetCommand("key2", "value2"),
		NewDeleteCommand("key1"),
		NewSetCommand("key3", "value3"),
	}

	for _, cmd := range commands {
		writer.Append(cmd)
	}

	// Close the writer to ensure all data is flushed
	writer.Close()

	// Create new reader to replay
	reader, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	// Replay and verify
	var replayed []Command
	cmdScanner, err := reader.CommandScanner()

	if err != nil {
		t.Fatalf("Failed to create commands scanner")
	}

	defer cmdScanner.Close()

	for cmdScanner.Scan() {
		replayed = append(replayed, cmdScanner.Command())
	}

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify we got all commands back
	if len(replayed) != len(commands) {
		t.Errorf("Expected %d commands, got %d", len(commands), len(replayed))
	}

	// Verify each command matches
	for i, expected := range commands {
		if i >= len(replayed) {
			break
		}
		got := replayed[i]

		if got.Op != expected.Op {
			t.Errorf("Command %d: expected Op %d, got %d", i, expected.Op, got.Op)
		}
		if got.Key != expected.Key {
			t.Errorf("Command %d: expected Key %s, got %s", i, expected.Key, got.Key)
		}
		if got.Value != expected.Value {
			t.Errorf("Command %d: expected Value %s, got %s", i, expected.Value, got.Value)
		}
	}
}

// TestWalWriter_EmptyFile tests replaying from an empty WAL file
func TestWalWriter_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "empty.wal")

	writer, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer writer.Close()

	// Don't append anything, just replay empty file
	var replayed []Command
	cmdScanner, err := writer.CommandScanner()

	if err != nil {
		t.Fatalf("Failed to create commands scanner")
	}

	defer cmdScanner.Close()

	for cmdScanner.Scan() {
		replayed = append(replayed, cmdScanner.Command())
	}

	err = cmdScanner.Err()

	if err != nil {
		t.Fatalf("Failed to replay empty WAL: %v", err)
	}

	if len(replayed) != 0 {
		t.Errorf("Expected 0 commands from empty WAL, got %d", len(replayed))
	}
}

// TestWalWriter_NonExistentFile tests that replaying a non-existent file returns error
func TestWalWriter_NonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "nonexistent.wal")

	writer := &WalWriter{filePath: walPath}

	_, err := writer.CommandScanner()

	if err == nil {
		t.Error("Expected error when replaying non-existent file, got nil")
	}
}

// TestWalWriter_PersistenceAcrossRestarts simulates multiple restarts
func TestWalWriter_PersistenceAcrossRestarts(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "persist.wal")

	// First session: write some data
	{
		writer, err := NewWalWriter(walPath)
		if err != nil {
			t.Fatalf("Session 1: Failed to create WAL writer: %v", err)
		}

		writer.Append(NewSetCommand("session1", "data1"))
		writer.Close()
	}

	// Second session: append more data
	{
		writer, err := NewWalWriter(walPath)
		if err != nil {
			t.Fatalf("Session 2: Failed to create WAL writer: %v", err)
		}

		writer.Append(NewSetCommand("session2", "data2"))
		writer.Close()
	}

	// Third session: replay all data
	{
		writer, err := NewWalWriter(walPath)
		if err != nil {
			t.Fatalf("Session 3: Failed to create WAL writer: %v", err)
		}
		defer writer.Close()

		var replayed []Command
		cmdScanner, err := writer.CommandScanner()

		if err != nil {
			t.Fatal("Failed to create command scanner")
		}

		defer cmdScanner.Close()

		for cmdScanner.Scan() {
			replayed = append(replayed, cmdScanner.Command())
		}

		if err != nil {
			t.Fatalf("Failed to replay WAL: %v", err)
		}

		// Should have both commands from both sessions
		if len(replayed) != 2 {
			t.Errorf("Expected 2 commands, got %d", len(replayed))
		}

		if len(replayed) >= 1 && replayed[0].Key != "session1" {
			t.Errorf("Expected first command key 'session1', got '%s'", replayed[0].Key)
		}
		if len(replayed) >= 2 && replayed[1].Key != "session2" {
			t.Errorf("Expected second command key 'session2', got '%s'", replayed[1].Key)
		}
	}
}

// TestWalWriter_InvalidDirectory tests creating WAL in non-existent directory
func TestWalWriter_InvalidDirectory(t *testing.T) {
	walPath := "/nonexistent/directory/wal.log"

	writer, err := NewWalWriter(walPath)

	if err == nil {
		writer.Close()
		t.Error("Expected error when creating WAL in non-existent directory, got nil")
	}
}

// TestWalWriter_MixedOperations tests a realistic sequence of SET and DELETE operations
func TestWalWriter_MixedOperations(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "mixed.wal")

	writer, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}

	// Simulate realistic KV store operations
	operations := []Command{
		NewSetCommand("user:1", "alice"),
		NewSetCommand("user:2", "bob"),
		NewSetCommand("counter", "1"),
		NewSetCommand("counter", "2"), // Update
		NewDeleteCommand("user:1"),    // Delete
		NewSetCommand("user:3", "charlie"),
	}

	for _, op := range operations {
		writer.Append(op)
	}
	writer.Close()

	// Replay and verify
	reader, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	var replayed []Command
	cmdScanner, err := reader.CommandScanner()

	if err != nil {
		t.Fatal("Failed to create command scanner")
	}

	defer cmdScanner.Close()

	for cmdScanner.Scan() {
		replayed = append(replayed, cmdScanner.Command())
	}

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if len(replayed) != len(operations) {
		t.Errorf("Expected %d operations, got %d", len(operations), len(replayed))
	}

	// Verify specific operations
	if len(replayed) >= 5 && replayed[4].Op != OpDELETE {
		t.Errorf("Expected DELETE operation at index 4, got %d", replayed[4].Op)
	}
	if len(replayed) >= 5 && replayed[4].Key != "user:1" {
		t.Errorf("Expected DELETE key 'user:1', got '%s'", replayed[4].Key)
	}
}

// TestWalWriter_LargeNumberOfCommands tests performance with many commands
func TestWalWriter_LargeNumberOfCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large test in short mode")
	}

	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "large.wal")

	writer, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}

	// Write many commands
	numCommands := 10000
	for i := 0; i < numCommands; i++ {
		key := "key" + string(rune('0'+i%10))
		value := "value" + string(rune('0'+i%10))
		writer.Append(NewSetCommand(key, value))
	}
	writer.Close()

	// Verify we can replay all of them
	reader, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	count := 0
	cmdScanner, err := reader.CommandScanner()

	if err != nil {
		t.Fatal("Failed to create command scanner")
	}

	defer cmdScanner.Close()

	for cmdScanner.Scan() {
		count += 1
	}

	err = cmdScanner.Err()

	if err != nil {
		t.Fatalf("Failed to replay large WAL: %v", err)
	}

	if count != numCommands {
		t.Errorf("Expected %d commands, got %d", numCommands, count)
	}
}

// TestWalWriter_FilePermissions tests that created file has correct permissions
func TestWalWriter_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "perms.wal")

	writer, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	writer.Close()

	// Check file permissions
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	expectedMode := os.FileMode(0644)
	if info.Mode().Perm() != expectedMode {
		t.Errorf("Expected file permissions %v, got %v", expectedMode, info.Mode().Perm())
	}
}

// TestWalWriter_WhitespaceHandling tests that keys and values with spaces work correctly
func TestWalWriter_WhitespaceHandling(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "whitespace.wal")

	writer, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}

	// Test various whitespace scenarios
	commands := []Command{
		NewSetCommand("simple", "no spaces"),
		NewSetCommand("key with spaces", "value with spaces"),
		NewSetCommand("user profile", "John Doe from New York"),
		NewSetCommand("sentence", "The quick brown fox jumps over the lazy dog"),
		NewSetCommand("leading spaces", "  value"),
		NewSetCommand("trailing spaces", "value  "),
		NewSetCommand("multiple  spaces", "value  with  gaps"),
		NewDeleteCommand("key with spaces"),
	}

	for _, cmd := range commands {
		writer.Append(cmd)
	}
	writer.Close()

	// Replay and verify all commands with whitespace are preserved
	reader, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL reader: %v", err)
	}
	defer reader.Close()

	var replayed []Command

	cmdScanner, err := reader.CommandScanner()

	if err != nil {
		t.Fatal("Failed to create command scanner")
	}

	defer cmdScanner.Close()

	for cmdScanner.Scan() {
		replayed = append(replayed, cmdScanner.Command())
	}

	err = cmdScanner.Err()

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if len(replayed) != len(commands) {
		t.Errorf("Expected %d commands, got %d", len(commands), len(replayed))
	}

	// Verify each command matches exactly, including whitespace
	for i, expected := range commands {
		if i >= len(replayed) {
			break
		}
		got := replayed[i]

		if got.Op != expected.Op {
			t.Errorf("Command %d: expected Op %d, got %d", i, expected.Op, got.Op)
		}
		if got.Key != expected.Key {
			t.Errorf("Command %d: expected Key %q, got %q", i, expected.Key, got.Key)
		}
		if got.Value != expected.Value {
			t.Errorf("Command %d: expected Value %q, got %q", i, expected.Value, got.Value)
		}
	}
}

// === Unit Tests for Serialization/Deserialization (No File I/O) ===

// TestCommand_Serialize_SET tests serialization of SET commands
func TestCommand_Serialize_SET(t *testing.T) {
	cmd := NewSetCommand("mykey", "myvalue")
	got := cmd.Serialize()
	want := "0\tmykey\tmyvalue\n"

	if got != want {
		t.Errorf("Serialize() = %q, want %q", got, want)
	}
}

// TestCommand_Serialize_DELETE tests serialization of DELETE commands
func TestCommand_Serialize_DELETE(t *testing.T) {
	cmd := NewDeleteCommand("mykey")
	got := cmd.Serialize()
	want := "1\tmykey\n"

	if got != want {
		t.Errorf("Serialize() = %q, want %q", got, want)
	}
}

// TestCommand_Serialize_SpecialCharacters tests serialization with special characters
func TestCommand_Serialize_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{
			name:  "colon in key",
			key:   "user:123",
			value: "alice",
			want:  "0\tuser:123\talice\n",
		},
		{
			name:  "spaces in value",
			key:   "message",
			value: "hello world",
			want:  "0\tmessage\thello world\n",
		},
		{
			name:  "spaces in key",
			key:   "first name",
			value: "alice",
			want:  "0\tfirst name\talice\n",
		},
		{
			name:  "multiple spaces",
			key:   "sentence",
			value: "the quick brown fox",
			want:  "0\tsentence\tthe quick brown fox\n",
		},
		{
			name:  "numeric value",
			key:   "counter",
			value: "12345",
			want:  "0\tcounter\t12345\n",
		},
		{
			name:  "empty value",
			key:   "empty",
			value: "",
			want:  "0\tempty\t\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewSetCommand(tt.key, tt.value)
			got := cmd.Serialize()
			if got != tt.want {
				t.Errorf("Serialize() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDeserializeCmd_SET tests deserialization of SET commands
func TestDeserializeCmd_SET(t *testing.T) {
	input := "0\tmykey\tmyvalue"
	cmd, err := DeserializeCmd(input)

	if err != nil {
		t.Fatalf("DeserializeCmd() error = %v, want nil", err)
	}

	if cmd.Op != OpSET {
		t.Errorf("Op = %d, want %d", cmd.Op, OpSET)
	}
	if cmd.Key != "mykey" {
		t.Errorf("Key = %q, want %q", cmd.Key, "mykey")
	}
	if cmd.Value != "myvalue" {
		t.Errorf("Value = %q, want %q", cmd.Value, "myvalue")
	}
}

// TestDeserializeCmd_DELETE tests deserialization of DELETE commands
func TestDeserializeCmd_DELETE(t *testing.T) {
	input := "1\tmykey"
	cmd, err := DeserializeCmd(input)

	if err != nil {
		t.Fatalf("DeserializeCmd() error = %v, want nil", err)
	}

	if cmd.Op != OpDELETE {
		t.Errorf("Op = %d, want %d", cmd.Op, OpDELETE)
	}
	if cmd.Key != "mykey" {
		t.Errorf("Key = %q, want %q", cmd.Key, "mykey")
	}
	if cmd.Value != "" {
		t.Errorf("Value = %q, want empty string", cmd.Value)
	}
}

// TestDeserializeCmd_RoundTrip tests that serialize -> deserialize is idempotent
func TestDeserializeCmd_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		cmd  Command
	}{
		{
			name: "SET command",
			cmd:  NewSetCommand("key1", "value1"),
		},
		{
			name: "DELETE command",
			cmd:  NewDeleteCommand("key2"),
		},
		{
			name: "SET with special chars",
			cmd:  NewSetCommand("user:123", "alice@example.com"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			serialized := tt.cmd.Serialize()

			// Remove trailing newline for deserialization
			serialized = strings.TrimSuffix(serialized, "\n")

			// Deserialize
			deserialized, err := DeserializeCmd(serialized)
			if err != nil {
				t.Fatalf("DeserializeCmd() error = %v", err)
			}

			// Compare
			if deserialized.Op != tt.cmd.Op {
				t.Errorf("Op = %d, want %d", deserialized.Op, tt.cmd.Op)
			}
			if deserialized.Key != tt.cmd.Key {
				t.Errorf("Key = %q, want %q", deserialized.Key, tt.cmd.Key)
			}
			if deserialized.Value != tt.cmd.Value {
				t.Errorf("Value = %q, want %q", deserialized.Value, tt.cmd.Value)
			}
		})
	}
}

// TestDeserializeCmd_InvalidFormats tests error handling for invalid formats
func TestDeserializeCmd_InvalidFormats(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "only opcode",
			input: "0",
		},
		{
			name:  "too many fields",
			input: "0\tk\tv\textra",
		},
		{
			name:  "invalid opcode",
			input: "invalid\tmykey",
		},
		{
			name:  "SET missing value",
			input: "0\tmykey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeserializeCmd(tt.input)
			if err == nil {
				t.Error("DeserializeCmd() expected error, got nil")
			}
		})
	}
}

// TestNewSetCommand tests the SET command constructor
func TestNewSetCommand(t *testing.T) {
	cmd := NewSetCommand("testkey", "testvalue")

	if cmd.Op != OpSET {
		t.Errorf("Op = %d, want %d", cmd.Op, OpSET)
	}
	if cmd.Key != "testkey" {
		t.Errorf("Key = %q, want %q", cmd.Key, "testkey")
	}
	if cmd.Value != "testvalue" {
		t.Errorf("Value = %q, want %q", cmd.Value, "testvalue")
	}
}

// TestNewDeleteCommand tests the DELETE command constructor
func TestNewDeleteCommand(t *testing.T) {
	cmd := NewDeleteCommand("testkey")

	if cmd.Op != OpDELETE {
		t.Errorf("Op = %d, want %d", cmd.Op, OpDELETE)
	}
	if cmd.Key != "testkey" {
		t.Errorf("Key = %q, want %q", cmd.Key, "testkey")
	}
	if cmd.Value != "" {
		t.Errorf("Value = %q, want empty string", cmd.Value)
	}
}

// TestOpCode_Values tests that OpCode constants have expected values
func TestOpCode_Values(t *testing.T) {
	if OpSET != 0 {
		t.Errorf("OpSET = %d, want 0", OpSET)
	}
	if OpDELETE != 1 {
		t.Errorf("OpDELETE = %d, want 1", OpDELETE)
	}
}
