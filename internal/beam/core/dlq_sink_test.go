// package core validates the DLQSinkDoFn atomic bundle lifecycle.
package core

import (
	"context"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestDLQSinkDoFn_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dlqCount   int
		wantCommit bool
		wantAbort  bool
	}{
		{
			name:       "writes dlq records and commits temp file",
			dlqCount:   2,
			wantCommit: true,
			wantAbort:  false,
		},
		{
			name:       "empty bundle does not create or commit temp file",
			dlqCount:   0,
			wantCommit: false,
			wantAbort:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStorageWriter{}
			fn := NewDLQSinkDoFn(fake, "gs://test-bucket/dlq")

			ctx := context.Background()
			// NOTE: No StartBundle call — Beam creates a new DoFn instance per bundle.

			for i := 0; i < tc.dlqCount; i++ {
				dlq := &domain.DeadLetterRecord{
					RawPayload:   "invalid,row",
					ErrorMessage: "cannot cast to int64",
					FailedColumn: "id",
					SourceFile:   "file1.csv",
					FailedAt:     time.Now().UTC(),
				}
				if err := fn.ProcessElement(ctx, dlq); err != nil {
					t.Fatalf("ProcessElement failed: %v", err)
				}
			}

			if err := fn.FinishBundle(ctx); err != nil {
				t.Fatalf("FinishBundle failed: %v", err)
			}

			if fake.tempCommitted != tc.wantCommit {
				t.Errorf("tempCommitted = %v, want %v", fake.tempCommitted, tc.wantCommit)
			}
			if fake.tempAborted != tc.wantAbort {
				t.Errorf("tempAborted = %v, want %v", fake.tempAborted, tc.wantAbort)
			}
		})
	}
}
