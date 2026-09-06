package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var _ ports.StorageReader = (*LocalStorage)(nil)
var _ ports.StorageWriter = (*LocalStorage)(nil)
var _ ports.StorageBackend = (*LocalStorage)(nil)

type LocalStorage struct{}

func NewLocalStorage() *LocalStorage {
	return &LocalStorage{}
}

func (s *LocalStorage) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	tracer := otel.Tracer("local-storage")
	_, span := tracer.Start(ctx, "LocalStorage.Open")
	defer span.End()
	span.SetAttributes(attribute.String("storage.uri", uri))

	f, err := os.Open(uri)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return f, nil
}

func (s *LocalStorage) List(ctx context.Context, uriPattern string) ([]string, error) {
	tracer := otel.Tracer("local-storage")
	_, span := tracer.Start(ctx, "LocalStorage.List")
	defer span.End()
	span.SetAttributes(attribute.String("storage.pattern", uriPattern))

	matches, err := filepath.Glob(uriPattern)
	if err != nil {
		err = fmt.Errorf("failed to glob files with pattern %s: %w", uriPattern, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if len(matches) == 0 {
		// If exact file exists
		if _, err := os.Stat(uriPattern); err == nil {
			matches = []string{uriPattern}
		}
	}
	span.SetAttributes(attribute.Int("storage.match_count", len(matches)))
	return matches, nil
}

func (s *LocalStorage) CreateTemp(ctx context.Context, finalURI string) (string, io.WriteCloser, error) {
	tracer := otel.Tracer("local-storage")
	_, span := tracer.Start(ctx, "LocalStorage.CreateTemp")
	defer span.End()
	span.SetAttributes(attribute.String("storage.final_uri", finalURI))

	dir := filepath.Dir(finalURI)
	if err := os.MkdirAll(dir, 0755); err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", dir, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", nil, err
	}
	base := filepath.Base(finalURI)
	tempName := fmt.Sprintf(".temp-%s-%s", uuid.NewString(), base)
	tempURI := filepath.Join(dir, tempName)

	f, err := os.Create(tempURI)
	if err != nil {
		err = fmt.Errorf("failed to create temp file %s: %w", tempURI, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", nil, err
	}
	span.SetAttributes(attribute.String("storage.temp_uri", tempURI))
	return tempURI, f, nil
}

func (s *LocalStorage) CommitTemp(ctx context.Context, tempURI, finalURI string) error {
	tracer := otel.Tracer("local-storage")
	_, span := tracer.Start(ctx, "LocalStorage.CommitTemp")
	defer span.End()
	span.SetAttributes(
		attribute.String("storage.temp_uri", tempURI),
		attribute.String("storage.final_uri", finalURI),
	)

	dir := filepath.Dir(finalURI)
	if err := os.MkdirAll(dir, 0755); err != nil {
		err = fmt.Errorf("failed to create target dir %s: %w", dir, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := os.Rename(tempURI, finalURI); err != nil {
		err = fmt.Errorf("failed to rename temp file %s to %s: %w", tempURI, finalURI, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (s *LocalStorage) AbortTemp(ctx context.Context, tempURI string) error {
	tracer := otel.Tracer("local-storage")
	_, span := tracer.Start(ctx, "LocalStorage.AbortTemp")
	defer span.End()
	span.SetAttributes(attribute.String("storage.temp_uri", tempURI))

	if err := os.Remove(tempURI); err != nil && !os.IsNotExist(err) {
		err = fmt.Errorf("failed to remove temp file %s: %w", tempURI, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// Size returns the file size in bytes using os.Stat in O(1).
func (s *LocalStorage) Size(ctx context.Context, uri string) (int64, error) {
	tracer := otel.Tracer("local-storage")
	_, span := tracer.Start(ctx, "LocalStorage.Size")
	defer span.End()
	span.SetAttributes(attribute.String("storage.uri", uri))

	cleanPath := filepath.Clean(uri)
	fi, err := os.Stat(cleanPath)
	if err != nil {
		err = fmt.Errorf("failed stating local file %q: %w", uri, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}
	return fi.Size(), nil
}
