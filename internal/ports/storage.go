// Package ports defines domain-level abstract contracts and interfaces
// for storage, streaming, codecs, and sinks.
package ports

import (
	"context"
	"io"
)

type StorageReader interface {
	Open(ctx context.Context, uri string) (io.ReadCloser, error)
	List(ctx context.Context, uriPattern string) ([]string, error)
	Size(ctx context.Context, uri string) (int64, error)
}

type StorageWriter interface {
	CreateTemp(ctx context.Context, finalURI string) (tempURI string, w io.WriteCloser, err error)
	CommitTemp(ctx context.Context, tempURI, finalURI string) error
	AbortTemp(ctx context.Context, tempURI string) error
}

// StorageBackend is a combined contract that a single backend (LocalStorage or GCSStorage)
// must implement to serve both read and write orchestration without unsafe type assertions.
type StorageBackend interface {
	StorageReader
	StorageWriter
}
