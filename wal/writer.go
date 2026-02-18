package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type WalManager interface {
	Append(cmd Command)
	CommandScanner() (*CommandScanner, error)
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

type CommandScanner struct {
	scanner *bufio.Scanner
	lastCmd Command
	lastErr error
	closer  io.Closer
}

func (c *CommandScanner) Scan() bool {
	for c.scanner.Scan() {
		line := c.scanner.Text()

		if line == "" {
			continue
		}

		cmd, err := DeserializeCmd(line)

		if err != nil {
			c.lastErr = fmt.Errorf("Failed to parse WAL line: %s", line)
			return false
		}

		c.lastCmd = cmd
		return true
	}

	c.lastErr = c.scanner.Err()

	return false

}

func (c *CommandScanner) Command() Command {
	return c.lastCmd
}

func (c *CommandScanner) Err() error {
	return c.lastErr
}

func (c *CommandScanner) Close() error {
	return c.closer.Close()
}

func (w *WalWriter) CommandScanner() (*CommandScanner, error) {
	file, err := os.Open(w.filePath)

	if err != nil {
		return nil, err
	}

	return &CommandScanner{
		scanner: bufio.NewScanner(file),
		closer:  file,
	}, nil
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
