package fio

import (
	"os"
	"testing"
)

func TestFileIO_write(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "fileio_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	fileIO, err := NewFileIOManager(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create FileIO: %v", err)
	}
	defer fileIO.Close()

	// Test Write
	data := []byte("Hello, world!")
	n, err := fileIO.Write(data)
	if err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, but wrote %d", len(data), n)
	}

	// Test Size
	size, err := fileIO.Size()
	if err != nil {
		t.Fatalf("Failed to get file size: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("Expected file size %d, but got %d", len(data), size)
	}

	// Test ReadAt
	offset := int64(0)
	readBufAt := make([]byte, len(data))
	n, err = fileIO.ReadAt(readBufAt, offset)
	if err != nil {
		t.Fatalf("Failed to read from file at offset: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to read %d bytes, but read %d", len(data), n)
	}
	if string(readBufAt) != string(data) {
		t.Errorf("Expected data %s at offset %d, but got %s", data, offset, readBufAt)
	}

	// Test Sync
	err = fileIO.Sync()
	if err != nil {
		t.Fatalf("Failed to sync file: %v", err)
	}
}
