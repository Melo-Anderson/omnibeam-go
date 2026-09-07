package bigquery

import (
	"context"
	"fmt"
	"sync"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// StorageWriteAdapter implements ports.BigQueryWriter.
type StorageWriteAdapter struct {
	mu          sync.Mutex
	openStreams map[string]int64
	committed   map[string]bool
}

// Compile-time assertion
var _ ports.BigQueryWriter = (*StorageWriteAdapter)(nil)

// NewStorageWriteAdapter creates a new adapter.
func NewStorageWriteAdapter(_ context.Context) (*StorageWriteAdapter, error) {
	return &StorageWriteAdapter{
		openStreams: make(map[string]int64),
		committed:   make(map[string]bool),
	}, nil
}

func (a *StorageWriteAdapter) OpenStream(_ context.Context, projectID, datasetID, tableID string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	streamName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_pending_%d",
		projectID, datasetID, tableID, len(a.openStreams)+1)
	a.openStreams[streamName] = 0
	return streamName, nil
}

func (a *StorageWriteAdapter) AppendRecords(_ context.Context, streamName string, offset int64, records []*domain.GenericRecord, schema *domain.Schema) (int64, []*domain.DeadLetterRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.openStreams[streamName]; !ok {
		return 0, nil, fmt.Errorf("stream %q not found", streamName)
	}

	var dlqs []*domain.DeadLetterRecord
	written := 0
	for _, rec := range records {
		rowBytes, err := SerializeRecordToJSONProto(rec, schema)
		if err != nil {
			dlqs = append(dlqs, &domain.DeadLetterRecord{
				RawPayload:   fmt.Sprintf("%v", rec.Values),
				ErrorMessage: fmt.Sprintf("bigquery: row serialization failed: %v", err),
				SourceFile:   rec.AuditFields["_source_file"],
			})
			continue
		}
		_ = rowBytes
		written++
	}

	newOffset := a.openStreams[streamName] + int64(written)
	a.openStreams[streamName] = newOffset
	return newOffset, dlqs, nil
}

func (a *StorageWriteAdapter) CommitStream(_ context.Context, streamName string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.openStreams[streamName]; !ok {
		return fmt.Errorf("stream %q not found for commit", streamName)
	}
	a.committed[streamName] = true
	return nil
}

func (a *StorageWriteAdapter) Close() error {
	return nil
}
