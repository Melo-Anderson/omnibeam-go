package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"pgregory.net/rapid"
)

func TestPipelineMetrics_ConservationInvariant(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		valid := rapid.Int64Range(0, 1_000_000).Draw(t, "valid")
		dlq := rapid.Int64Range(0, 1_000_000).Draw(t, "dlq")
		m := domain.PipelineMetrics{
			TotalRecordsRead: valid + dlq,
			RowsWritten:      valid,
			DeadLetterCount:  dlq,
		}
		if !m.IsStrictlyConserved() {
			t.Fatalf("conservation violated: Total=%d, Valid=%d, DLQ=%d",
				m.TotalRecordsRead, m.RowsWritten, m.DeadLetterCount)
		}
	})
}
