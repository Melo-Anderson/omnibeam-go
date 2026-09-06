package core_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
)

type fakeStorageWriter struct {
	created  map[string]*bytes.Buffer
	comitted map[string]string
	aborted  map[string]bool
	failTemp bool
}

func newFakeStorageWriter() *fakeStorageWriter {
	return &fakeStorageWriter{
		created:  make(map[string]*bytes.Buffer),
		comitted: make(map[string]string),
		aborted:  make(map[string]bool),
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

func (f *fakeStorageWriter) CreateTemp(_ context.Context, finalURI string) (string, io.WriteCloser, error) {
	if f.failTemp {
		return "", nil, fmt.Errorf("simulated temp create error")
	}
	tmpURI := finalURI + ".tmp"
	buf := &bytes.Buffer{}
	f.created[tmpURI] = buf
	return tmpURI, nopWriteCloser{buf}, nil
}

func (f *fakeStorageWriter) CommitTemp(_ context.Context, tempURI, finalURI string) error {
	f.comitted[tempURI] = finalURI
	return nil
}

func (f *fakeStorageWriter) AbortTemp(_ context.Context, tempURI string) error {
	f.aborted[tempURI] = true
	return nil
}

func TestBundleFileStager_Lifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("Happy path with written records", func(t *testing.T) {
		storage := newFakeStorageWriter()
		stager := core.NewBundleFileStager(storage, "/tmp/out", "jsonl", "none", false)

		stager.StartBundle(ctx)
		tempURI, w, err := stager.EnsureOpen(ctx)
		if err != nil || w == nil {
			t.Fatalf("EnsureOpen failed: %v", err)
		}

		stager.IncrementRecord()
		if err := stager.CommitOrAbort(ctx, nil); err != nil {
			t.Fatalf("CommitOrAbort failed: %v", err)
		}

		if len(storage.comitted) != 1 || storage.comitted[tempURI] == "" {
			t.Errorf("expected tempURI committed, got %+v", storage.comitted)
		}
	})

	t.Run("Idle bundle with 0 records aborts staging file", func(t *testing.T) {
		storage := newFakeStorageWriter()
		stager := core.NewBundleFileStager(storage, "/tmp/out", "jsonl", "none", false)

		stager.StartBundle(ctx)
		tempURI, _, err := stager.EnsureOpen(ctx)
		if err != nil {
			t.Fatalf("EnsureOpen failed: %v", err)
		}

		// 0 records written
		if err := stager.CommitOrAbort(ctx, nil); err != nil {
			t.Fatalf("CommitOrAbort failed: %v", err)
		}

		if !storage.aborted[tempURI] {
			t.Error("expected 0-record staging file to be aborted")
		}
	})

	t.Run("Unopened bundle commits nothing", func(t *testing.T) {
		storage := newFakeStorageWriter()
		stager := core.NewBundleFileStager(storage, "/tmp/out", "jsonl", "none", false)
		stager.StartBundle(ctx)

		if err := stager.CommitOrAbort(ctx, nil); err != nil {
			t.Errorf("unexpected error on unopened bundle: %v", err)
		}
		if len(storage.comitted) != 0 || len(storage.aborted) != 0 {
			t.Error("expected 0 commits/aborts for unopened bundle")
		}
	})

	t.Run("Abort explicitly cleans up temp file", func(t *testing.T) {
		storage := newFakeStorageWriter()
		stager := core.NewBundleFileStager(storage, "/tmp/out", "jsonl", "none", false)
		stager.StartBundle(ctx)
		tempURI, _, _ := stager.EnsureOpen(ctx)

		if err := stager.Abort(ctx); err != nil {
			t.Fatalf("Abort failed: %v", err)
		}
		if !storage.aborted[tempURI] {
			t.Error("expected file aborted")
		}
	})
}

func TestBundleFileStager_Drain(t *testing.T) {
	ctx := context.Background()
	storage := newFakeStorageWriter()
	stager := core.NewBundleFileStager(storage, "/tmp/out", "parquet", "none", false)

	stager.StartBundle(ctx)
	_, _, err := stager.EnsureOpen(ctx)
	if err != nil {
		t.Fatalf("EnsureOpen failed: %v", err)
	}
	stager.IncrementRecord()

	if err := stager.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	if len(storage.comitted) != 1 {
		t.Errorf("expected 1 committed file after Drain, got %d", len(storage.comitted))
	}
}

