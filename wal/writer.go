package wal

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type WalManager interface {
	Append(cmd Command)
	Replay(func(Command)) error
	Truncate() error
}

type WalWriter struct {
	file     *os.File
	filePath string
}

type OpCode int

const (
	OpSET OpCode = iota
	OpDELETE
)

type Command struct {
	Op    OpCode
	Key   string
	Value string
}

func NewSetCommand(key, value string) Command {
	return Command{
		Op:    OpSET,
		Key:   key,
		Value: value,
	}
}
func NewDeleteCommand(key string) Command {
	return Command{
		Op:  OpDELETE,
		Key: key,
	}
}

func DeserializeCmd(record string) (Command, error) {
	parts := strings.Split(record, "\t")

	if len(parts) < 2 || len(parts) > 3 {
		return Command{}, fmt.Errorf("Invalid WAL format: %s", record)
	}

	cmd := Command{}

	opInt, err := strconv.Atoi(parts[0])

	if err != nil {
		return Command{}, fmt.Errorf("Invalid Op code: %s", parts[0])
	}

	cmd.Op = OpCode(opInt)

	cmd.Key = parts[1]
	if cmd.Op == OpSET {
		if len(parts) < 3 {
			return Command{}, fmt.Errorf("SET command missing value: %s", record)
		}
		cmd.Value = parts[2]
	}

	return cmd, nil
}

func (c Command) Serialize() string {
	if c.Op == OpSET {
		return fmt.Sprintf("%d\t%s\t%s\n", c.Op, c.Key, c.Value)
	}

	return fmt.Sprintf("%d\t%s\n", c.Op, c.Key)
}

const FILE_NAME = "data/wal.log"

func NewWalWriter(filePath string) (*WalWriter, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	return &WalWriter{
		file:     file,
		filePath: filePath,
	}, nil
}

func (w *WalWriter) Append(cmd Command) {
	writer := bufio.NewWriter(w.file)
	_, err := writer.WriteString(cmd.Serialize())

	if err != nil {
		fmt.Printf("Failed to write to WAL log because of err: %v", err)
	}

	writer.Flush()
}

func (w *WalWriter) Replay(callback func(Command)) error {
	file, err := os.Open(w.filePath)

	if err != nil {
		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			continue
		}

		cmd, err := DeserializeCmd(line)

		if err != nil {
			return fmt.Errorf("Failed to parse WAL line: %s", line)
		}

		callback(cmd)
	}

	return scanner.Err()
}

func (w *WalWriter) Close() {
	w.file.Close()
}

func (w *WalWriter) Truncate() error {
	w.file.Close()

	file, err := os.Create(w.filePath)

	if err != nil {
		return err
	}

	w.file = file

	return nil
}
