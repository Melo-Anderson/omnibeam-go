// Package storage implements filesystem and object storage adapters.
package storage

import (
	"fmt"
	"strings"
)

// GCSPath represents a parsed Google Cloud Storage bucket and object locator.
type GCSPath struct {
	Bucket string
	Object string
}

// ParseGCSURI parses a "gs://bucket/object" URI into its bucket and object components.
func ParseGCSURI(rawURI string) (*GCSPath, error) {
	normalized := strings.ReplaceAll(rawURI, "\\", "/")
	if !strings.HasPrefix(normalized, "gs://") {
		return nil, fmt.Errorf("invalid gcs uri %q: must start with 'gs://'", rawURI)
	}

	trimmed := strings.TrimPrefix(normalized, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 2 || parts[0] == "" || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("invalid gcs uri %q: missing bucket or object key", rawURI)
	}

	return &GCSPath{
		Bucket: parts[0],
		Object: parts[1],
	}, nil
}
