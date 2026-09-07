// Package storage implements filesystem and object storage adapters.
package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	gcs_client "cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var _ ports.StorageReader = (*GCSStorage)(nil)
var _ ports.StorageWriter = (*GCSStorage)(nil)
var _ ports.StorageBackend = (*GCSStorage)(nil)

// GCSStorage implements cloud-native object storage for Google Cloud Storage.
type GCSStorage struct {
	client *gcs_client.Client
}

// NewGCSStorage constructs a GCSStorage adapter with ADC or custom client options.
// Automatically applies emulator options if STORAGE_EMULATOR_HOST is set.
func NewGCSStorage(ctx context.Context, opts ...option.ClientOption) (*GCSStorage, error) {
	var finalOpts []option.ClientOption
	if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
		endpoint := fmt.Sprintf("http://%s/storage/v1/", emulatorHost)
		finalOpts = append(finalOpts, option.WithEndpoint(endpoint), option.WithoutAuthentication())
	}
	finalOpts = append(finalOpts, opts...)

	client, err := gcs_client.NewClient(ctx, finalOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed creating gcs client: %w", err)
	}
	return &GCSStorage{client: client}, nil
}

// Size returns the GCS object size in bytes querying metadata in O(1).
func (s *GCSStorage) Size(ctx context.Context, uri string) (int64, error) {
	tracer := otel.Tracer("storage")
	ctx, span := tracer.Start(ctx, "GCSStorage.Size")
	defer span.End()
	span.SetAttributes(attribute.String("storage.uri", uri))

	parsed, err := ParseGCSURI(uri)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}
	attrs, err := s.client.Bucket(parsed.Bucket).Object(parsed.Object).Attrs(ctx)
	if err != nil {
		err = fmt.Errorf("failed fetching gcs object attrs for %q: %w", uri, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}
	return attrs.Size, nil
}

// Close closes the underlying GCS client.
func (s *GCSStorage) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Open opens a GCS object reader for streaming.
func (s *GCSStorage) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	tracer := otel.Tracer("storage")
	ctx, span := tracer.Start(ctx, "GCSStorage.Open")
	defer span.End()

	parsed, err := ParseGCSURI(uri)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(
		attribute.String("gcs.bucket", parsed.Bucket),
		attribute.String("gcs.object", parsed.Object),
	)

	rc, err := s.client.Bucket(parsed.Bucket).Object(parsed.Object).NewReader(ctx)
	if err != nil {
		err = fmt.Errorf("failed opening gcs object %q: %w", uri, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	return rc, nil
}

// List returns a list of GCS object URIs matching the given prefix.
func (s *GCSStorage) List(ctx context.Context, uriPattern string) ([]string, error) {
	tracer := otel.Tracer("storage")
	ctx, span := tracer.Start(ctx, "GCSStorage.List")
	defer span.End()

	parsed, err := ParseGCSURI(uriPattern)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(
		attribute.String("gcs.bucket", parsed.Bucket),
		attribute.String("gcs.prefix", parsed.Object),
	)

	it := s.client.Bucket(parsed.Bucket).Objects(ctx, &gcs_client.Query{Prefix: parsed.Object})
	var matches []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			err = fmt.Errorf("failed listing gcs objects with pattern %q: %w", uriPattern, err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		matches = append(matches, fmt.Sprintf("gs://%s/%s", parsed.Bucket, attrs.Name))
	}
	span.SetAttributes(attribute.Int("gcs.result_count", len(matches)))
	return matches, nil
}

// CreateTemp initiates an atomic staging write session to GCS.
func (s *GCSStorage) CreateTemp(ctx context.Context, finalURI string) (string, io.WriteCloser, error) {
	tracer := otel.Tracer("storage")
	ctx, span := tracer.Start(ctx, "GCSStorage.CreateTemp")
	defer span.End()

	parsed, err := ParseGCSURI(finalURI)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", nil, err
	}

	dir := path.Dir(parsed.Object)
	base := path.Base(parsed.Object)
	tempObjName := fmt.Sprintf(".temp-%s-%s", uuid.NewString(), base)
	if dir != "." && dir != "/" {
		tempObjName = path.Join(dir, tempObjName)
	}

	span.SetAttributes(
		attribute.String("gcs.bucket", parsed.Bucket),
		attribute.String("gcs.staging_object", tempObjName),
	)

	tempURI := fmt.Sprintf("gs://%s/%s", parsed.Bucket, tempObjName)
	w := s.client.Bucket(parsed.Bucket).Object(tempObjName).NewWriter(ctx)

	return tempURI, w, nil
}

// CommitTemp atomically promotes the staging object to final destination via server-side rewrite.
func (s *GCSStorage) CommitTemp(ctx context.Context, tempURI, finalURI string) error {
	tracer := otel.Tracer("storage")
	ctx, span := tracer.Start(ctx, "GCSStorage.CommitTemp")
	defer span.End()

	src, err := ParseGCSURI(tempURI)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	dst, err := ParseGCSURI(finalURI)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.SetAttributes(
		attribute.String("gcs.bucket", src.Bucket),
		attribute.String("gcs.staging_object", src.Object),
		attribute.String("gcs.final_object", dst.Object),
	)

	srcObj := s.client.Bucket(src.Bucket).Object(src.Object)
	dstObj := s.client.Bucket(dst.Bucket).Object(dst.Object)

	if _, err := dstObj.CopierFrom(srcObj).Run(ctx); err != nil {
		err = fmt.Errorf("failed server-side copy from %q to %q: %w", tempURI, finalURI, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := srcObj.Delete(ctx); err != nil {
		err = fmt.Errorf("failed cleaning up staging object %q: %w", tempURI, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// AbortTemp deletes the staging object upon write failure.
func (s *GCSStorage) AbortTemp(ctx context.Context, tempURI string) error {
	tracer := otel.Tracer("storage")
	ctx, span := tracer.Start(ctx, "GCSStorage.AbortTemp")
	defer span.End()

	parsed, err := ParseGCSURI(tempURI)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.SetAttributes(
		attribute.String("gcs.bucket", parsed.Bucket),
		attribute.String("gcs.staging_object", parsed.Object),
	)

	obj := s.client.Bucket(parsed.Bucket).Object(parsed.Object)
	if err := obj.Delete(ctx); err != nil && !strings.Contains(err.Error(), "object doesn't exist") {
		err = fmt.Errorf("failed aborting staging object %q: %w", tempURI, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}
