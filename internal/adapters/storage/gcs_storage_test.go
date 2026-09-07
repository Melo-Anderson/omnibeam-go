package storage_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/storage"
	"google.golang.org/api/option"
)

func TestGCSStorage_UnitLifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("NewGCSStorage with WithoutAuthentication does not error on creation", func(t *testing.T) {
		gcs, err := storage.NewGCSStorage(ctx, option.WithoutAuthentication())
		if err != nil {
			t.Fatalf("NewGCSStorage: %v", err)
		}
		defer gcs.Close()

		// Test Close doesn't panic
		if err := gcs.Close(); err != nil {
			t.Errorf("unexpected error on Close: %v", err)
		}
	})

	t.Run("Size returns error on invalid GCS URI", func(t *testing.T) {
		gcs, err := storage.NewGCSStorage(ctx, option.WithoutAuthentication())
		if err != nil {
			t.Fatalf("NewGCSStorage: %v", err)
		}
		defer gcs.Close()

		_, err = gcs.Size(ctx, "invalid-uri")
		if err == nil {
			t.Error("expected error for non-GCS URI, got nil")
		}
	})

	t.Run("Open, CreateTemp, AbortTemp return error for invalid GCS URI", func(t *testing.T) {
		gcs, _ := storage.NewGCSStorage(ctx, option.WithoutAuthentication())
		defer gcs.Close()

		_, err := gcs.Open(ctx, "invalid-uri")
		if err == nil {
			t.Error("expected error for invalid uri in Open")
		}

		_, _, err = gcs.CreateTemp(ctx, "invalid-uri")
		if err == nil {
			t.Error("expected error for invalid uri in CreateTemp")
		}

		err = gcs.CommitTemp(ctx, "invalid-temp", "invalid-final")
		if err == nil {
			t.Error("expected error for invalid uri in CommitTemp")
		}

		err = gcs.AbortTemp(ctx, "invalid-temp")
		if err == nil {
			t.Error("expected error for invalid uri in AbortTemp")
		}

		_, err = gcs.List(ctx, "invalid-uri")
		if err == nil {
			t.Error("expected error for invalid uri in List")
		}
	})

	t.Run("Mock GCS HTTP Server operations", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("MOCK GCS REQUEST: Method=%s Path=%s Query=%s Header=%v", r.Method, r.URL.Path, r.URL.RawQuery, r.Header)
			w.Header().Set("Content-Type", "application/json")

			// Media download
			if r.Method == http.MethodGet && (r.URL.Path == "/my-bucket/data.csv" || r.URL.Query().Get("alt") == "media" || strings.Contains(r.URL.RawQuery, "alt=media")) {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Length", "18")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("gcs-payload-stream"))
				return
			}

			// Metadata query
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/o/data.csv") {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"bucket":"my-bucket","name":"data.csv","size":"18"}`))
				return
			}

			// Objects list
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/o") {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"items":[{"bucket":"my-bucket","name":"data.csv"}]}`))
				return
			}

			// Rewrite
			if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/rewriteTo/") {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"kind":"storage#rewriteResponse","done":true,"objectSize":"18"}`))
				return
			}

			// Delete
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			// Upload create session
			if r.Method == http.MethodPost {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"bucket":"my-bucket","name":"temp-file","size":"18"}`))
				return
			}

			w.WriteHeader(http.StatusOK)
		}))
		defer ts.Close()

		endpoint := ts.URL + "/storage/v1/"
		gcs, err := storage.NewGCSStorage(ctx, option.WithEndpoint(endpoint), option.WithoutAuthentication())
		if err != nil {
			t.Fatalf("NewGCSStorage with mock: %v", err)
		}
		defer gcs.Close()

		// Test Size
		size, err := gcs.Size(ctx, "gs://my-bucket/data.csv")
		if err != nil || size != 18 {
			t.Errorf("expected size 18, got %d (err: %v)", size, err)
		}

		// Test List
		list, err := gcs.List(ctx, "gs://my-bucket/data.csv")
		if err != nil || len(list) != 1 {
			t.Errorf("expected 1 listed object, got %v (err: %v)", list, err)
		}

		// Test Open
		rc, err := gcs.Open(ctx, "gs://my-bucket/data.csv")
		if err != nil {
			t.Fatalf("gcs.Open: %v", err)
		}
		defer rc.Close()
		body, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(body) != "gcs-payload-stream" {
			t.Errorf("expected 'gcs-payload-stream', got %q", string(body))
		}

		// Test CreateTemp with root object and nested dir
		tmpURI1, w1, err := gcs.CreateTemp(ctx, "gs://my-bucket/out.csv")
		if err != nil || !strings.HasPrefix(tmpURI1, "gs://my-bucket/.temp-") {
			t.Errorf("CreateTemp root object failed: %v, %v", tmpURI1, err)
		}
		_ = w1.Close()

		tmpURI2, w2, err := gcs.CreateTemp(ctx, "gs://my-bucket/nested/dir/out.csv")
		if err != nil || !strings.Contains(tmpURI2, "nested/dir/.temp-") {
			t.Errorf("CreateTemp nested object failed: %v, %v", tmpURI2, err)
		}
		_ = w2.Close()

		// Test CommitTemp
		err = gcs.CommitTemp(ctx, "gs://my-bucket/.temp-123-out.csv", "gs://my-bucket/out.csv")
		if err != nil {
			t.Errorf("CommitTemp failed: %v", err)
		}

		// Test AbortTemp
		err = gcs.AbortTemp(ctx, "gs://my-bucket/.temp-123-out.csv")
		if err != nil {
			t.Errorf("AbortTemp failed: %v", err)
		}
	})

	t.Run("NewGCSStorage respects STORAGE_EMULATOR_HOST", func(t *testing.T) {
		t.Setenv("STORAGE_EMULATOR_HOST", "localhost:9099")
		gcs, err := storage.NewGCSStorage(ctx)
		if err != nil {
			t.Fatalf("NewGCSStorage with emulator: %v", err)
		}
		defer gcs.Close()
	})
}
