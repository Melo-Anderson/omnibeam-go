package core

import (
	"bytes"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestOriginCombinerFn_Unit(t *testing.T) {
	fn := &OriginCombinerFn{}

	acc := fn.CreateAccumulator()
	acc = fn.AddInput(acc, false) // valid
	acc = fn.AddInput(acc, true)  // dlq
	acc = fn.AddInput(acc, false) // valid

	if acc.TotalRead != 3 || acc.Valid != 2 || acc.DLQ != 1 {
		t.Fatalf("unexpected accumulator: %+v", acc)
	}

	other := OriginAccumulator{TotalRead: 2, Valid: 1, DLQ: 1}
	merged := fn.MergeAccumulators(acc, other)

	out := fn.ExtractOutput(merged)
	if out.TotalRead != 5 || out.Valid != 3 || out.DLQ != 2 {
		t.Fatalf("unexpected output: %+v", out)
	}
}

func TestMetricItem_CustomBinaryCoder(t *testing.T) {
	tests := []struct {
		name string
		item MetricItem
	}{
		{
			name: "valid record file origin",
			item: MetricItem{Origin: "gs://bucket/path/data.csv.gz", IsDLQ: false},
		},
		{
			name: "dlq record table origin",
			item: MetricItem{Origin: "postgres.public.orders", IsDLQ: true},
		},
		{
			name: "empty origin",
			item: MetricItem{Origin: "", IsDLQ: false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := EncodeMetricItem(tc.item, &buf); err != nil {
				t.Fatalf("EncodeMetricItem failed: %v", err)
			}
			decoded, err := DecodeMetricItem(&buf)
			if err != nil {
				t.Fatalf("DecodeMetricItem failed: %v", err)
			}
			if decoded.Origin != tc.item.Origin || decoded.IsDLQ != tc.item.IsDLQ {
				t.Errorf("mismatch: got %+v, want %+v", decoded, tc.item)
			}
		})
	}
}

func init() {
	beam.RegisterFunction(metricItemToKVFn)
}

func metricItemToKVFn(item MetricItem) (string, bool) {
	return item.Origin, item.IsDLQ
}

func TestTwoStageMetrics_PipelineIntegration(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	inputs := beam.Create(s,
		MetricItem{Origin: "orders.csv", IsDLQ: false},
		MetricItem{Origin: "orders.csv", IsDLQ: false},
		MetricItem{Origin: "orders.csv", IsDLQ: true},
		MetricItem{Origin: "customers.csv", IsDLQ: false},
		MetricItem{Origin: "customers.csv", IsDLQ: true},
	)
	kvs := beam.ParDo(s, metricItemToKVFn, inputs)

	perOrigin := beam.CombinePerKey(s, &OriginCombinerFn{}, kvs)
	summaries := beam.ParDo(s, originKVToSummaryFn, perOrigin)
	finalMetrics := beam.Combine(s, &MetricsSummaryCombinerFn{}, summaries)

	expected := domain.PipelineMetrics{
		TotalRecordsRead: 5,
		RowsWritten:      3,
		DeadLetterCount:  2,
		OriginMetrics: map[string]domain.OriginMetric{
			"orders.csv":    {TotalRead: 3, Valid: 2, DLQ: 1},
			"customers.csv": {TotalRead: 2, Valid: 1, DLQ: 1},
		},
		ColumnNullCounts:   make(map[string]int64),
		InvalidValueCounts: make(map[string]int64),
		CustomMetrics:      make(map[string]any),
	}

	passert.Equals(s, finalMetrics, expected)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("ptest failed executing two-stage metrics pipeline: %v", err)
	}
}
