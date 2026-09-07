// Package storage implements filesystem and object storage adapters.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var ErrBackendNotConfigured = errors.New("storage backend not configured for this scheme")

var _ ports.StorageReader = (*StorageResolver)(nil)
var _ ports.StorageWriter = (*StorageResolver)(nil)
var _ ports.StorageBackend = (*StorageResolver)(nil)

// StorageResolver dynamically routes storage operations based on URI scheme.
type StorageResolver struct {
	local ports.StorageBackend
	gcs   ports.StorageBackend
}

// NewStorageResolver constructs a StorageResolver with injected local and cloud backends.
func NewStorageResolver(local, gcs ports.StorageBackend) *StorageResolver {
	return &StorageResolver{
		local: local,
		gcs:   gcs,
	}
}

func (r *StorageResolver) selectBackend(uri string) (ports.StorageBackend, error) {
	if strings.HasPrefix(uri, "gs://") || strings.HasPrefix(uri, "gs:\\") {
		if r.gcs == nil {
			return nil, fmt.Errorf("%w: gcs storage requested for %q", ErrBackendNotConfigured, uri)
		}
		return r.gcs, nil
	}
	if strings.Contains(uri, "://") {
		return nil, fmt.Errorf("%w: unsupported scheme for %q", ErrBackendNotConfigured, uri)
	}
	if r.local == nil {
		return nil, fmt.Errorf("%w: local storage requested for %q", ErrBackendNotConfigured, uri)
	}
	return r.local, nil
}

// Open delegates read stream creation to the resolved backend.
func (r *StorageResolver) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	b, err := r.selectBackend(uri)
	if err != nil {
		return nil, err
	}
	return b.Open(ctx, uri)
}

// List delegates path matching to the resolved backend.
func (r *StorageResolver) List(ctx context.Context, uriPattern string) ([]string, error) {
	b, err := r.selectBackend(uriPattern)
	if err != nil {
		return nil, err
	}
	return b.List(ctx, uriPattern)
}

// Size delegates size query to the resolved backend.
func (r *StorageResolver) Size(ctx context.Context, uri string) (int64, error) {
	b, err := r.selectBackend(uri)
	if err != nil {
		return 0, err
	}
	return b.Size(ctx, uri)
}

// CreateTemp delegates atomic staging creation to the resolved backend.
func (r *StorageResolver) CreateTemp(ctx context.Context, finalURI string) (string, io.WriteCloser, error) {
	b, err := r.selectBackend(finalURI)
	if err != nil {
		return "", nil, err
	}
	return b.CreateTemp(ctx, finalURI)
}

// CommitTemp delegates atomic commit to the resolved backend.
func (r *StorageResolver) CommitTemp(ctx context.Context, tempURI, finalURI string) error {
	b, err := r.selectBackend(finalURI)
	if err != nil {
		return err
	}
	return b.CommitTemp(ctx, tempURI, finalURI)
}

// AbortTemp delegates staging cleanup to the resolved backend.
func (r *StorageResolver) AbortTemp(ctx context.Context, tempURI string) error {
	b, err := r.selectBackend(tempURI)
	if err != nil {
		return err
	}
	return b.AbortTemp(ctx, tempURI)
}
