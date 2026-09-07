package storage_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/storage"
)

func TestLocalStorage_AtomicWriteCycle(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	store := storage.NewLocalStorage()

	finalPath := filepath.Join(tmpDir, "data", "final.parquet")

	t.Run("CreateTemp, Write, and CommitTemp successfully", func(t *testing.T) {
		tempPath, w, err := store.CreateTemp(ctx, finalPath)
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		if _, err := io.WriteString(w, "test content"); err != nil {
			t.Fatalf("failed to write content: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("failed to close writer: %v", err)
		}

		if err := store.CommitTemp(ctx, tempPath, finalPath); err != nil {
			t.Fatalf("failed to commit temp file: %v", err)
		}

		if _, err := os.Stat(finalPath); err != nil {
			t.Errorf("expected final file to exist: %v", err)
		}
		if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
			t.Errorf("expected temp file to be removed after commit")
		}
	})

	t.Run("CreateTemp and AbortTemp cleans up without leaving files", func(t *testing.T) {
		abortFinalPath := filepath.Join(tmpDir, "data", "aborted.parquet")
		tempPath, w, err := store.CreateTemp(ctx, abortFinalPath)
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		_ = w.Close()

		if err := store.AbortTemp(ctx, tempPath); err != nil {
			t.Fatalf("failed to abort temp file: %v", err)
		}

		if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
			t.Errorf("expected temp file to be deleted")
		}
		if _, err := os.Stat(abortFinalPath); !os.IsNotExist(err) {
			t.Errorf("expected final file to not exist")
		}
	})
}

func TestLocalStorage_Size(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	store := storage.NewLocalStorage()

	filePath := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello storage size")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed writing test file: %v", err)
	}

	size, err := store.Size(ctx, filePath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), size)
	}

	_, err = store.Size(ctx, filepath.Join(tmpDir, "nonexistent.txt"))
	if err == nil {
		t.Errorf("expected error for nonexistent file, got nil")
	}
}

func TestLocalStorage_OpenAndList(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	store := storage.NewLocalStorage()

	f1 := filepath.Join(tmpDir, "file1.csv")
	f2 := filepath.Join(tmpDir, "file2.csv")
	_ = os.WriteFile(f1, []byte("a,b\n1,2\n"), 0644)
	_ = os.WriteFile(f2, []byte("a,b\n3,4\n"), 0644)

	t.Run("Open existing file", func(t *testing.T) {
		rc, err := store.Open(ctx, f1)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer rc.Close()
		content, _ := io.ReadAll(rc)
		if string(content) != "a,b\n1,2\n" {
			t.Errorf("unexpected content: %s", string(content))
		}
	})

	t.Run("Open non-existent file", func(t *testing.T) {
		_, err := store.Open(ctx, filepath.Join(tmpDir, "missing.csv"))
		if err == nil {
			t.Error("expected error for missing file, got nil")
		}
	})

	t.Run("List with glob", func(t *testing.T) {
		matches, err := store.List(ctx, filepath.Join(tmpDir, "*.csv"))
		if err != nil || len(matches) != 2 {
			t.Errorf("expected 2 matches, got %d (err: %v)", len(matches), err)
		}
	})

	t.Run("List with exact file", func(t *testing.T) {
		matches, err := store.List(ctx, f1)
		if err != nil || len(matches) != 1 {
			t.Errorf("expected 1 match for exact file, got %d", len(matches))
		}
	})

	t.Run("List directory returns all files inside", func(t *testing.T) {
		matches, err := store.List(ctx, filepath.Join(tmpDir, "*"))
		if err != nil || len(matches) < 2 {
			t.Errorf("expected at least 2 files from dir listing, got %v (err: %v)", matches, err)
		}
	})

	t.Run("List non-existent pattern returns empty slice", func(t *testing.T) {
		matches, err := store.List(ctx, filepath.Join(tmpDir, "non_existent_subdir", "*.csv"))
		if err != nil || len(matches) != 0 {
			t.Errorf("expected 0 matches for non-existent subdir, got %v", matches)
		}
	})

	t.Run("CommitTemp error on missing temp file", func(t *testing.T) {
		err := store.CommitTemp(ctx, filepath.Join(tmpDir, "missing-temp"), filepath.Join(tmpDir, "dst"))
		if err == nil {
			t.Error("expected error for missing temp file in CommitTemp")
		}
	})
}

func TestLocalStorage_TracingSpans(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	store := storage.NewLocalStorage()

	target := filepath.Join(tmpDir, "traced.txt")
	tmp, w, err := store.CreateTemp(ctx, target)
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	_, _ = w.Write([]byte("traced data"))
	_ = w.Close()

	if err := store.CommitTemp(ctx, tmp, target); err != nil {
		t.Fatalf("CommitTemp: %v", err)
	}

	rc, err := store.Open(ctx, target)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = rc.Close()

	if _, err := store.Size(ctx, target); err != nil {
		t.Fatalf("Size: %v", err)
	}

	if _, err := store.List(ctx, filepath.Join(tmpDir, "*")); err != nil {
		t.Fatalf("List: %v", err)
	}

	// Test error span paths
	_ = store.AbortTemp(ctx, filepath.Join(tmpDir, "non_existent_temp"))
	_, _ = store.Open(ctx, filepath.Join(tmpDir, "missing.file"))
	_, _ = store.Size(ctx, filepath.Join(tmpDir, "missing.file"))
}
