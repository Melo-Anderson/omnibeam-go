package formatters

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/compression"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"github.com/omnibeam/dataflow-compute-go/pkg/ioutils"
)

type FileWriterContext struct {
	finalURI     string
	tempURI      string
	storageW     io.WriteCloser
	compCloser   io.Closer
	targetWriter io.Writer
	countHashW   *ioutils.CountingHashWriter
	schema       *domain.Schema
	recordsCount int64
}

type DelimitedFileWriter struct {
	storage     ports.StorageWriter
	formatter   ports.RecordFormatter
	compression string
}

func NewDelimitedFileWriter(
	storage ports.StorageWriter,
	formatter ports.RecordFormatter,
	compression string,
) *DelimitedFileWriter {
	return &DelimitedFileWriter{
		storage:     storage,
		formatter:   formatter,
		compression: strings.ToLower(compression),
	}
}

func (w *DelimitedFileWriter) Open(ctx context.Context, finalURI string, schema *domain.Schema) (*FileWriterContext, error) {
	tempURI, storageW, err := w.storage.CreateTemp(ctx, finalURI)
	if err != nil {
		return nil, fmt.Errorf("failed creating temp file: %w", err)
	}

	countHash := ioutils.NewCountingHashWriter(storageW)

	targetWriter, compCloser, err := compression.WrapCompressor(countHash, w.compression)
	if err != nil {
		_ = storageW.Close()
		_ = w.storage.AbortTemp(ctx, tempURI)
		return nil, err
	}

	wCtx := &FileWriterContext{
		finalURI:     finalURI,
		tempURI:      tempURI,
		storageW:     storageW,
		compCloser:   compCloser,
		targetWriter: targetWriter,
		countHashW:   countHash,
		schema:       schema,
		recordsCount: 0,
	}

	header, err := w.formatter.FormatHeader(schema)
	if err != nil {
		_ = w.abort(ctx, wCtx)
		return nil, fmt.Errorf("failed formatting header: %w", err)
	}
	if len(header) > 0 {
		if _, err := targetWriter.Write(header); err != nil {
			_ = w.abort(ctx, wCtx)
			return nil, fmt.Errorf("failed writing header: %w", err)
		}
	}

	return wCtx, nil
}

func (w *DelimitedFileWriter) Write(wCtx *FileWriterContext, rec *domain.GenericRecord) error {
	rowBytes, err := w.formatter.FormatRecord(rec, wCtx.schema)
	if err != nil {
		return fmt.Errorf("failed formatting record: %w", err)
	}
	if len(rowBytes) > 0 {
		if _, err := wCtx.targetWriter.Write(rowBytes); err != nil {
			return fmt.Errorf("failed writing record: %w", err)
		}
	}
	wCtx.recordsCount++
	return nil
}

func (w *DelimitedFileWriter) Close(ctx context.Context, wCtx *FileWriterContext) (domain.SinkMetrics, error) {
	if wCtx.compCloser != nil {
		if err := wCtx.compCloser.Close(); err != nil {
			_ = w.abort(ctx, wCtx)
			return domain.SinkMetrics{}, fmt.Errorf("failed closing compression writer: %w", err)
		}
	}
	if err := wCtx.storageW.Close(); err != nil {
		_ = w.abort(ctx, wCtx)
		return domain.SinkMetrics{}, fmt.Errorf("failed closing storage writer: %w", err)
	}

	if wCtx.recordsCount == 0 {
		_ = w.storage.AbortTemp(ctx, wCtx.tempURI)
		return domain.SinkMetrics{}, nil
	}

	if err := w.storage.CommitTemp(ctx, wCtx.tempURI, wCtx.finalURI); err != nil {
		_ = w.storage.AbortTemp(ctx, wCtx.tempURI)
		return domain.SinkMetrics{}, fmt.Errorf("failed committing temp file: %w", err)
	}

	return domain.SinkMetrics{
		RowsWritten:  wCtx.recordsCount,
		BytesWritten: wCtx.countHashW.Written(),
		FilesWritten: 1,
		Checksum:     wCtx.countHashW.HexSum(),
	}, nil
}

func (w *DelimitedFileWriter) abort(ctx context.Context, wCtx *FileWriterContext) error {
	if wCtx.compCloser != nil {
		_ = wCtx.compCloser.Close()
	}
	if wCtx.storageW != nil {
		_ = wCtx.storageW.Close()
	}
	return w.storage.AbortTemp(ctx, wCtx.tempURI)
}
