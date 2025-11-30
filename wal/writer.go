package wal

import (
	"bufio"
	"fmt"
	"os"
)

type WalManager interface {
	Append(cmd Command)
}

type WalWriter struct {
	file *os.File
}

type OpCode int

const (
	OpSET OpCode = iota
	OpDELETE
)

type Command struct {
	op    OpCode
	key   string
	value string
}

func NewSetCommand(key, value string) Command {
	return Command{
		op:    OpSET,
		key:   key,
		value: value,
	}
}
func NewDeleteCommand(key string) Command {
	return Command{
		op:  OpDELETE,
		key: key,
	}
}

func (c Command) Serialize() string {
	if c.op == OpSET {
		return fmt.Sprintf("op:%d key:%s value:%s\n", c.op, c.key, c.value)
	}

	return fmt.Sprintf("op:%d key:%s\n", c.op, c.key)
}

func NewWalWriter(fileName string) (*WalWriter, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	return &WalWriter{
		file: file,
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

func (w *WalWriter) Close() {
	w.file.Close()
}
