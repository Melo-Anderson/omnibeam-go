package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteErrorFile_WritesCauseMessage(t *testing.T) {
	tempDir := t.TempDir()
	cause := errors.New("database connection failed: timeout")

	writeErrorFile(tempDir, cause)

	errPath := filepath.Join(tempDir, "error.txt")
	content, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("expected error.txt to be created: %v", err)
	}
	if string(content) != cause.Error() {
		t.Fatalf("expected content %q, got %q", cause.Error(), string(content))
	}
}

func TestWriteErrorFile_IgnoresNilErrorOrGCS(t *testing.T) {
	tempDir := t.TempDir()
	writeErrorFile(tempDir, nil)
	if _, err := os.Stat(filepath.Join(tempDir, "error.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected no file written for nil error")
	}

	writeErrorFile("gs://my-bucket/out", errors.New("some error"))
	// Should not crash or attempt local directory creation
}
