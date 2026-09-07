package domain_test

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestPipelineMetrics_IsStrictlyConserved(t *testing.T) {
	t.Run("Strict conservation holds", func(t *testing.T) {
		m := domain.PipelineMetrics{
			TotalRecordsRead: 1050,
			RowsWritten:      1000,
			DeadLetterCount:  50,
		}
		if !m.IsStrictlyConserved() {
			t.Errorf("expected conservation to be true for 1050 == 1000 + 50")
		}
	})

	t.Run("Conservation fails if counts do not balance", func(t *testing.T) {
		m := domain.PipelineMetrics{
			TotalRecordsRead: 1050,
			RowsWritten:      1000,
			DeadLetterCount:  49, // 1 record silently dropped
		}
		if m.IsStrictlyConserved() {
			t.Errorf("expected conservation to be false for 1050 != 1000 + 49")
		}
	})
}

func TestPipelineMetrics_MarshalJSON(t *testing.T) {
	m := domain.NewPipelineMetrics("p_test", "run_123")
	m.TotalRecordsRead = 1000
	m.RowsWritten = 950
	m.DeadLetterCount = 50
	m.BytesWritten = 2048590
	m.FilesWritten = 2
	m.Checksum = "a8f5f167f44f4964e6c998dee827110c"
	m.ExecutionDurationMs = 1250
	m.RecordsPerSecond = 800.0
	m.ThroughputMBPerSecond = 1.63
	m.ColumnNullCounts["null_count_id_cliente"] = 3
	m.ColumnNullCounts["null_count_email"] = 0
	m.InvalidValueCounts["invalid_value_count_status"] = 2
	m.CustomMetrics["custom_flag"] = "ok"

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	jsonStr := string(b)

	expectedKeys := []string{
		`"pipeline_id":"p_test"`,
		`"run_id":"run_123"`,
		`"row_count":950`,
		`"rows_written":950`,
		`"dead_letter_count":50`,
		`"bytes_written":2048590`,
		`"files_written":2`,
		`"checksum":"a8f5f167f44f4964e6c998dee827110c"`,
		`"execution_duration_ms":1250`,
		`"records_per_second":800`,
		`"throughput_mb_per_second":1.63`,
		`"null_count_id_cliente":3`,
		`"null_count_email":0`,
		`"invalid_value_count_status":2`,
		`"custom_flag":"ok"`,
	}

	for _, k := range expectedKeys {
		if !strings.Contains(jsonStr, k) {
			t.Errorf("Expected JSON to contain %s, got: %s", k, jsonStr)
		}
	}

	if strings.Contains(jsonStr, "column_null_counts") || strings.Contains(jsonStr, "invalid_value_counts") {
		t.Errorf("Internal map keys should not appear in serialized JSON: %s", jsonStr)
	}
}

func TestPipelineMetrics_RecordHelpers(t *testing.T) {
	m := domain.NewPipelineMetrics("pipe-1", "run-1")
	m.RecordRead()
	if m.TotalRecordsRead != 1 {
		t.Errorf("expected 1 read, got %d", m.TotalRecordsRead)
	}
	m.RecordWritten()
	if m.RowsWritten != 1 {
		t.Errorf("expected 1 written, got %d", m.RowsWritten)
	}
	m.RecordDeadLetter()
	if m.DeadLetterCount != 1 {
		t.Errorf("expected 1 dead letter, got %d", m.DeadLetterCount)
	}
	m.RecordAPICall()
	if m.APICallsMade != 1 {
		t.Errorf("expected 1 api call, got %d", m.APICallsMade)
	}
}

func TestPipelineMetrics_ConcurrentSafety(t *testing.T) {
	m := domain.NewPipelineMetrics("test-pipe", "run-1")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RecordRead()
			m.RecordWritten()
			m.RecordAPICall()
			m.RecordDeadLetter()
			m.AddColumnNull("col_a")
			m.AddInvalidValue("col_b")
			_ = m.Snapshot()
			_, _ = m.MarshalJSON()
		}()
	}
	wg.Wait()

	snap := m.Snapshot()
	if snap.TotalRecordsRead != 100 || snap.RowsWritten != 100 || snap.DeadLetterCount != 100 {
		t.Fatalf("unexpected counts: %+v", snap)
	}
}

func TestPipelineMetrics_OriginConservation(t *testing.T) {
	m := domain.NewPipelineMetrics("p_test", "run_123")
	m.OriginMetrics["file_a.csv"] = domain.OriginMetric{TotalRead: 100, Valid: 95, DLQ: 5}
	m.OriginMetrics["file_b.csv"] = domain.OriginMetric{TotalRead: 50, Valid: 50, DLQ: 0}

	if !m.IsConservedPerOrigin() {
		t.Errorf("expected per-origin conservation to hold")
	}

	snap := m.Snapshot()
	if snap.OriginMetrics["file_a.csv"].TotalRead != 100 {
		t.Errorf("expected file_a snapshot to have 100 total read")
	}

	// Corrupt one origin
	m.OriginMetrics["file_a.csv"] = domain.OriginMetric{TotalRead: 100, Valid: 90, DLQ: 5}
	if m.IsConservedPerOrigin() {
		t.Errorf("expected per-origin conservation to fail when counts do not match")
	}
}
