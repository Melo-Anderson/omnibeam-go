package domain

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

type SinkMetrics struct {
	RowsWritten  int64
	BytesWritten int64
	FilesWritten int64
	Checksum     string
}

type OriginMetric struct {
	TotalRead int64 `json:"total_read"`
	Valid     int64 `json:"valid"`
	DLQ       int64 `json:"dlq"`
}

type PipelineMetrics struct {
	mu                    *sync.RWMutex           `json:"-"`
	PipelineID            string                  `json:"pipeline_id"`
	RunID                 string                  `json:"run_id"`
	TotalRecordsRead      int64                   `json:"total_records_read"`
	RowCount              int64                   `json:"row_count"`
	RowsWritten           int64                   `json:"rows_written"`
	DeadLetterCount       int64                   `json:"dead_letter_count"`
	BytesWritten          int64                   `json:"bytes_written"`
	FilesWritten          int64                   `json:"files_written"`
	APICallsMade          int64                   `json:"api_calls_made,omitempty"`
	Checksum              string                  `json:"checksum"`
	ExecutionDurationMs   int64                   `json:"execution_duration_ms"`
	RecordsPerSecond      float64                 `json:"records_per_second"`
	ThroughputMBPerSecond float64                 `json:"throughput_mb_per_second"`
	OriginMetrics         map[string]OriginMetric `json:"origin_metrics,omitempty"`
	ColumnNullCounts      map[string]int64        `json:"-"`
	InvalidValueCounts    map[string]int64        `json:"-"`
	CustomMetrics         map[string]any          `json:"-"`
}

func NewPipelineMetrics(pipelineID, runID string) *PipelineMetrics {
	return &PipelineMetrics{
		mu:                 &sync.RWMutex{},
		PipelineID:         pipelineID,
		RunID:              runID,
		OriginMetrics:      make(map[string]OriginMetric),
		ColumnNullCounts:   make(map[string]int64),
		InvalidValueCounts: make(map[string]int64),
		CustomMetrics:      make(map[string]any),
	}
}

func (m *PipelineMetrics) RecordRead() {
	atomic.AddInt64(&m.TotalRecordsRead, 1)
}

func (m *PipelineMetrics) RecordWritten() {
	newVal := atomic.AddInt64(&m.RowsWritten, 1)
	atomic.StoreInt64(&m.RowCount, newVal)
}

func (m *PipelineMetrics) RecordAPICall() {
	atomic.AddInt64(&m.APICallsMade, 1)
}

func (m *PipelineMetrics) RecordDeadLetter() {
	atomic.AddInt64(&m.DeadLetterCount, 1)
}

func (m *PipelineMetrics) AddColumnNull(col string) {
	if m.mu != nil {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	if m.ColumnNullCounts != nil {
		m.ColumnNullCounts[col]++
	}
}

func (m *PipelineMetrics) AddInvalidValue(col string) {
	if m.mu != nil {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	if m.InvalidValueCounts != nil {
		m.InvalidValueCounts[col]++
	}
}

func (m *PipelineMetrics) IsStrictlyConserved() bool {
	return atomic.LoadInt64(&m.TotalRecordsRead) == (atomic.LoadInt64(&m.RowsWritten) + atomic.LoadInt64(&m.DeadLetterCount))
}

func (m *PipelineMetrics) IsConservedPerOrigin() bool {
	if m.mu != nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}
	for _, om := range m.OriginMetrics {
		if om.TotalRead != (om.Valid + om.DLQ) {
			return false
		}
	}
	return true
}

func (m *PipelineMetrics) Snapshot() PipelineMetrics {
	if m.mu != nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}
	var originMetrics map[string]OriginMetric
	if len(m.OriginMetrics) > 0 {
		originMetrics = make(map[string]OriginMetric, len(m.OriginMetrics))
		for k, v := range m.OriginMetrics {
			originMetrics[k] = v
		}
	}
	var nullCounts map[string]int64
	if len(m.ColumnNullCounts) > 0 {
		nullCounts = make(map[string]int64, len(m.ColumnNullCounts))
		for k, v := range m.ColumnNullCounts {
			nullCounts[k] = v
		}
	}
	var invalidCounts map[string]int64
	if len(m.InvalidValueCounts) > 0 {
		invalidCounts = make(map[string]int64, len(m.InvalidValueCounts))
		for k, v := range m.InvalidValueCounts {
			invalidCounts[k] = v
		}
	}
	var custom map[string]any
	if len(m.CustomMetrics) > 0 {
		custom = make(map[string]any, len(m.CustomMetrics))
		for k, v := range m.CustomMetrics {
			custom[k] = v
		}
	}

	return PipelineMetrics{
		PipelineID:            m.PipelineID,
		RunID:                 m.RunID,
		TotalRecordsRead:      atomic.LoadInt64(&m.TotalRecordsRead),
		RowCount:              atomic.LoadInt64(&m.RowCount),
		RowsWritten:           atomic.LoadInt64(&m.RowsWritten),
		DeadLetterCount:       atomic.LoadInt64(&m.DeadLetterCount),
		BytesWritten:          atomic.LoadInt64(&m.BytesWritten),
		FilesWritten:          atomic.LoadInt64(&m.FilesWritten),
		APICallsMade:          atomic.LoadInt64(&m.APICallsMade),
		Checksum:              m.Checksum,
		ExecutionDurationMs:   m.ExecutionDurationMs,
		RecordsPerSecond:      m.RecordsPerSecond,
		ThroughputMBPerSecond: m.ThroughputMBPerSecond,
		OriginMetrics:         originMetrics,
		ColumnNullCounts:      nullCounts,
		InvalidValueCounts:    invalidCounts,
		CustomMetrics:         custom,
	}
}

func (m *PipelineMetrics) MarshalJSON() ([]byte, error) {
	snap := m.Snapshot()
	if snap.RowCount == 0 && snap.RowsWritten > 0 {
		snap.RowCount = snap.RowsWritten
	}
	type Alias PipelineMetrics
	a := (*Alias)(&snap)
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	if len(snap.ColumnNullCounts) == 0 && len(snap.InvalidValueCounts) == 0 && len(snap.CustomMetrics) == 0 {
		return b, nil
	}
	var root map[string]any
	if err := json.Unmarshal(b, &root); err != nil {
		return nil, err
	}
	for k, v := range snap.ColumnNullCounts {
		root[k] = v
	}
	for k, v := range snap.InvalidValueCounts {
		root[k] = v
	}
	for k, v := range snap.CustomMetrics {
		root[k] = v
	}
	return json.Marshal(root)
}
