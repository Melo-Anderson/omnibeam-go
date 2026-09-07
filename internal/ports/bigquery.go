package ports

import (
	"context"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// BigQueryWriter defines the contract for writing pending stream batches to BigQuery.
type BigQueryWriter interface {
	// OpenStream creates a Pending Write Stream for the target table.
	OpenStream(ctx context.Context, projectID, datasetID, tableID string) (string, error)
	// AppendRecords serializes and appends a batch of GenericRecords to the open pending stream.
	// Returns the new committed row offset. Row-level rejections or serialization failures are returned as DeadLetterRecords.
	AppendRecords(ctx context.Context, streamName string, offset int64, records []*domain.GenericRecord, schema *domain.Schema) (int64, []*domain.DeadLetterRecord, error)
	// CommitStream atomically finalizes and commits the pending stream to the table.
	CommitStream(ctx context.Context, streamName string) error
	// Close releases the underlying gRPC connection.
	Close() error
}

// BigQueryWriterProvider is a factory for obtaining BigQueryWriter instances on remote Beam workers.
// It is registered globally via SetGlobalBigQueryWriterProvider and called in DoFn.Setup.
type BigQueryWriterProvider func(ctx context.Context, projectID string) (BigQueryWriter, error)
