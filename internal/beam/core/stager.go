package core

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// BundleFileStager coordinates lazy staging, atomic commit, and error abort for Beam sink bundles.
type BundleFileStager struct {
	OutputDir   string `json:"output_dir"`
	Format      string `json:"format"`
	Compression string `json:"compression"`
	SingleFile  bool   `json:"single_file"`

	// Unexported state — recomposed in Setup() on remote workers
	storage     ports.StorageWriter
	tempURI     string
	finalURI    string
	writer      io.WriteCloser
	recordCount int64
}

// RecordCount returns the number of records written in the current bundle.
func (s *BundleFileStager) RecordCount() int64 {
	return s.recordCount
}

// NewBundleFileStager creates a new BundleFileStager with injected storage dependency.
func NewBundleFileStager(
	storage ports.StorageWriter,
	outputDir string,
	format string,
	compression string,
	singleFile bool,
) *BundleFileStager {
	return &BundleFileStager{
		storage:     storage,
		OutputDir:   strings.TrimRight(outputDir, "/"),
		Format:      strings.ToLower(format),
		Compression: strings.ToLower(compression),
		SingleFile:  singleFile,
	}
}

// Setup re-establishes storage factory on remote worker nodes after Beam serialization.
func (s *BundleFileStager) Setup(_ context.Context) error {
	if s.storage == nil && GetStorageFactory() != nil {
		s.storage = GetStorageFactory()(s.OutputDir)
	}
	return nil
}

// StartBundle resets bundle-level state for the active bundle.
func (s *BundleFileStager) StartBundle(_ context.Context) {
	s.recordCount = 0
	s.writer = nil
	s.tempURI = ""
	s.finalURI = ""
}

// EnsureOpen lazily creates the staging file on the first record to avoid empty files for idle bundles.
func (s *BundleFileStager) EnsureOpen(ctx context.Context) (string, io.WriteCloser, error) {
	if s.writer != nil {
		return s.tempURI, s.writer, nil
	}

	if s.storage == nil && GetStorageFactory() != nil {
		s.storage = GetStorageFactory()(s.OutputDir)
	}
	if s.storage == nil {
		return "", nil, fmt.Errorf("storage writer is not initialized")
	}

	s.finalURI = domain.BuildOutputURI(s.OutputDir, s.Format, s.Compression, s.SingleFile)
	tempURI, w, err := s.storage.CreateTemp(ctx, s.finalURI)
	if err != nil {
		return "", nil, fmt.Errorf("failed creating temp staging file: %w", err)
	}

	s.tempURI = tempURI
	s.writer = w
	return tempURI, w, nil
}

// IncrementRecord advances the written record counter.
func (s *BundleFileStager) IncrementRecord() {
	s.recordCount++
}

// Abort closes any open writer and deletes the staging temp file.
func (s *BundleFileStager) Abort(ctx context.Context) error {
	if s.writer != nil {
		_ = s.writer.Close()
		s.writer = nil
	}
	if s.tempURI != "" && s.storage != nil {
		return s.storage.AbortTemp(ctx, s.tempURI)
	}
	return nil
}

// CommitOrAbort atomically commits the staging file if records were written, or aborts if 0 records.
func (s *BundleFileStager) CommitOrAbort(ctx context.Context, closer io.Closer) error {
	if s.writer == nil {
		return nil
	}

	if closer != nil {
		if err := closer.Close(); err != nil {
			_ = s.Abort(ctx)
			return fmt.Errorf("failed closing staging wrapper: %w", err)
		}
	}
	if err := s.writer.Close(); err != nil {
		_ = s.Abort(ctx)
		return fmt.Errorf("failed closing staging writer: %w", err)
	}
	s.writer = nil

	if s.recordCount == 0 {
		return s.storage.AbortTemp(ctx, s.tempURI)
	}

	if err := s.storage.CommitTemp(ctx, s.tempURI, s.finalURI); err != nil {
		_ = s.storage.AbortTemp(ctx, s.tempURI)
		return fmt.Errorf("failed committing staging file: %w", err)
	}
	return nil
}

// Drain safely commits any open temp file during worker shutdown or preemption.
// It is semantically equivalent to FinishBundle — it calls CommitOrAbort with no
// external closer, ensuring in-flight data is atomically persisted before the
// Dataflow worker process exits.
func (s *BundleFileStager) Drain(ctx context.Context) error {
	return s.CommitOrAbort(ctx, nil)
}
