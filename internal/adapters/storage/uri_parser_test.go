package storage_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/storage"
)

func TestParseGCSURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		wantBucket  string
		wantObject  string
		expectError bool
	}{
		{
			name:        "Valid root object",
			uri:         "gs://my-bucket/data.csv",
			wantBucket:  "my-bucket",
			wantObject:  "data.csv",
			expectError: false,
		},
		{
			name:        "Valid nested object path",
			uri:         "gs://my-bucket/sub/folder/file.parquet",
			wantBucket:  "my-bucket",
			wantObject:  "sub/folder/file.parquet",
			expectError: false,
		},
		{
			name:        "Invalid scheme returns error",
			uri:         "s3://my-bucket/data.csv",
			expectError: true,
		},
		{
			name:        "Local path returns error",
			uri:         "/local/path/data.csv",
			expectError: true,
		},
		{
			name:        "Missing object returns error",
			uri:         "gs://my-bucket/",
			expectError: true,
		},
		{
			name:        "Missing bucket returns error",
			uri:         "gs:///data.csv",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := storage.ParseGCSURI(tc.uri)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error for URI %q, got nil", tc.uri)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parsed.Bucket != tc.wantBucket {
				t.Errorf("bucket mismatch: got %q, want %q", parsed.Bucket, tc.wantBucket)
			}
			if parsed.Object != tc.wantObject {
				t.Errorf("object mismatch: got %q, want %q", parsed.Object, tc.wantObject)
			}
		})
	}
}
