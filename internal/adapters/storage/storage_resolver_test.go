package storage_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/storage"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type fakeBackend struct {
	openedURI     string
	listedPattern string
	createdTemp   string
	committedTemp string
	committedDst  string
	abortedTemp   string
}

func (f *fakeBackend) Open(_ context.Context, uri string) (io.ReadCloser, error) {
	f.openedURI = uri
	return io.NopCloser(strings.NewReader("fake content")), nil
}

func (f *fakeBackend) List(_ context.Context, uriPattern string) ([]string, error) {
	f.listedPattern = uriPattern
	return []string{uriPattern}, nil
}

func (f *fakeBackend) CreateTemp(_ context.Context, finalURI string) (string, io.WriteCloser, error) {
	f.createdTemp = finalURI
	return ".temp-" + finalURI, nil, nil
}

func (f *fakeBackend) CommitTemp(_ context.Context, tempURI, finalURI string) error {
	f.committedTemp = tempURI
	f.committedDst = finalURI
	return nil
}

func (f *fakeBackend) AbortTemp(_ context.Context, tempURI string) error {
	f.abortedTemp = tempURI
	return nil
}

func (f *fakeBackend) Size(_ context.Context, uri string) (int64, error) {
	return 42, nil
}

var _ ports.StorageBackend = (*fakeBackend)(nil)

func TestStorageResolver(t *testing.T) {
	ctx := context.Background()
	local := &fakeBackend{}
	gcs := &fakeBackend{}

	resolver := storage.NewStorageResolver(local, gcs)

	t.Run("Dispatches gs:// URI to GCS backend", func(t *testing.T) {
		gcsURI := "gs://bucket/data.parquet"
		_, _ = resolver.Open(ctx, gcsURI)
		if gcs.openedURI != gcsURI {
			t.Errorf("expected GCS backend to receive Open for %q, got %q", gcsURI, gcs.openedURI)
		}
		if local.openedURI != "" {
			t.Errorf("expected local backend to remain untouched, got %q", local.openedURI)
		}
	})

	t.Run("Dispatches local path to Local backend", func(t *testing.T) {
		localPath := "/data/output/test.parquet"
		_, _, _ = resolver.CreateTemp(ctx, localPath)
		if local.createdTemp != localPath {
			t.Errorf("expected Local backend to receive CreateTemp for %q, got %q", localPath, local.createdTemp)
		}
	})

	t.Run("Dispatches CommitTemp and AbortTemp based on URI scheme", func(t *testing.T) {
		gcsTemp := "gs://bucket/.temp-123-file.parquet"
		gcsFinal := "gs://bucket/file.parquet"
		_ = resolver.CommitTemp(ctx, gcsTemp, gcsFinal)
		if gcs.committedDst != gcsFinal {
			t.Errorf("expected GCS commit to %q, got %q", gcsFinal, gcs.committedDst)
		}

		_ = resolver.AbortTemp(ctx, gcsTemp)
		if gcs.abortedTemp != gcsTemp {
			t.Errorf("expected GCS abort for %q, got %q", gcsTemp, gcs.abortedTemp)
		}
	})
}

func TestStorageResolver_SizeAndNilSafety(t *testing.T) {
	ctx := context.Background()
	local := &fakeBackend{}

	// Resolver configured without GCS backend
	resolver := storage.NewStorageResolver(local, nil)

	t.Run("Dispatches local Size query", func(t *testing.T) {
		sz, err := resolver.Size(ctx, "/path/to/local.csv")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sz != 42 {
			t.Errorf("expected size 42, got %d", sz)
		}
	})

	t.Run("Returns ErrBackendNotConfigured when GCS backend is nil", func(t *testing.T) {
		_, err := resolver.Size(ctx, "gs://my-bucket/file.parquet")
		if err == nil {
			t.Fatalf("expected error querying unconfigured GCS backend, got nil")
		}
		if !strings.Contains(err.Error(), "backend not configured") {
			t.Errorf("expected backend not configured error, got: %v", err)
		}
	})

	t.Run("List forwarding and unsupported scheme error", func(t *testing.T) {
		rFull := storage.NewStorageResolver(local, &fakeBackend{})
		listRes, err := rFull.List(ctx, "/local/*.csv")
		if err != nil || len(listRes) == 0 {
			t.Errorf("List local failed: %v", err)
		}
		gcsList, err := rFull.List(ctx, "gs://bucket/*.csv")
		if err != nil || len(gcsList) == 0 {
			t.Errorf("List GCS failed: %v", err)
		}

		// Unknown scheme
		_, err = rFull.Open(ctx, "ftp://example.com/file.csv")
		if err == nil {
			t.Error("expected error for unsupported scheme ftp://, got nil")
		}
	})
}
