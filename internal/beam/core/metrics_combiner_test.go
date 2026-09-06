package core

import (
	"bytes"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestMetricsCombinerFn(t *testing.T) {
	fn := &MetricsCombinerFn{}

	acc := fn.CreateAccumulator()
	if acc.TotalRead != 0 || acc.Valid != 0 || acc.DLQ != 0 {
		t.Errorf("expected zero accumulator, got %+v", acc)
	}

	acc = fn.AddInput(acc, MetricItem{Origin: "file_a.csv", IsDLQ: false}) // 1 valid
	acc = fn.AddInput(acc, MetricItem{Origin: "file_a.csv", IsDLQ: true})  // 1 DLQ
	acc = fn.AddInput(acc, MetricItem{Origin: "file_b.csv", IsDLQ: false}) // 1 valid

	if acc.TotalRead != 3 || acc.Valid != 2 || acc.DLQ != 1 {
		t.Errorf("unexpected accumulator counts: %+v", acc)
	}
	if acc.OriginMetrics["file_a.csv"].TotalRead != 2 || acc.OriginMetrics["file_a.csv"].Valid != 1 || acc.OriginMetrics["file_a.csv"].DLQ != 1 {
		t.Errorf("unexpected file_a origin metrics: %+v", acc.OriginMetrics["file_a.csv"])
	}
	if acc.OriginMetrics["file_b.csv"].TotalRead != 1 || acc.OriginMetrics["file_b.csv"].Valid != 1 || acc.OriginMetrics["file_b.csv"].DLQ != 0 {
		t.Errorf("unexpected file_b origin metrics: %+v", acc.OriginMetrics["file_b.csv"])
	}

	acc2 := MetricsAccumulator{
		TotalRead: 5,
		Valid:     4,
		DLQ:       1,
		OriginMetrics: map[string]domain.OriginMetric{
			"file_b.csv": {TotalRead: 5, Valid: 4, DLQ: 1},
		},
	}
	merged := fn.MergeAccumulators(acc, acc2)

	if merged.TotalRead != 8 || merged.Valid != 6 || merged.DLQ != 2 {
		t.Errorf("unexpected merged accumulator counts: %+v", merged)
	}
	if merged.OriginMetrics["file_b.csv"].TotalRead != 6 || merged.OriginMetrics["file_b.csv"].Valid != 5 || merged.OriginMetrics["file_b.csv"].DLQ != 1 {
		t.Errorf("unexpected merged file_b metrics: %+v", merged.OriginMetrics["file_b.csv"])
	}

	metrics := fn.ExtractOutput(merged)
	if metrics.TotalRecordsRead != 8 || metrics.RowsWritten != 6 || metrics.DeadLetterCount != 2 {
		t.Errorf("unexpected extracted domain metrics: %+v", metrics)
	}
	if !metrics.IsStrictlyConserved() {
		t.Errorf("expected strict conservation to hold")
	}
	if !metrics.IsConservedPerOrigin() {
		t.Errorf("expected per-origin conservation to hold")
	}
}

func TestMetricItem_CustomBinaryCoder(t *testing.T) {
	tests := []struct {
		name string
		item MetricItem
	}{
		{"valid record file origin", MetricItem{Origin: "gs://bucket/path/data.csv.gz", IsDLQ: false}},
		{"dlq record table origin", MetricItem{Origin: "postgres.public.orders", IsDLQ: true}},
		{"empty origin", MetricItem{Origin: "", IsDLQ: false}},
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

func TestPipeline_MetricsCombiner_Integration(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	inputs := beam.Create(s,
		MetricItem{Origin: "orders.csv", IsDLQ: false},
		MetricItem{Origin: "orders.csv", IsDLQ: false},
		MetricItem{Origin: "orders.csv", IsDLQ: true},
		MetricItem{Origin: "customers.csv", IsDLQ: false},
		MetricItem{Origin: "customers.csv", IsDLQ: true},
	)

	metricsPCol := beam.Combine(s, &MetricsCombinerFn{}, inputs)

	expected := domain.PipelineMetrics{
		TotalRecordsRead: 5,
		RowsWritten:      3,
		DeadLetterCount:  2,
		OriginMetrics: map[string]domain.OriginMetric{
			"orders.csv":    {TotalRead: 3, Valid: 2, DLQ: 1},
			"customers.csv": {TotalRead: 2, Valid: 1, DLQ: 1},
		},
	}

	passert.Equals(s, metricsPCol, expected)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("ptest failed executing metrics combiner pipeline: %v", err)
	}
}

