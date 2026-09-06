package core

import (
	"bytes"
	"errors"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*MetricItem)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*MetricItem)(nil)).Elem(), encMetricItem, decMetricItem)
	beam.RegisterType(reflect.TypeOf((*MetricsAccumulator)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*MetricsCombinerFn)(nil)).Elem())
}

// MetricItem captures record outcome alongside its originating source partition.
type MetricItem struct {
	Origin string `json:"origin"`
	IsDLQ  bool   `json:"is_dlq"`
}

func encMetricItem(v MetricItem) ([]byte, error) {
	var buf bytes.Buffer
	err := EncodeMetricItem(v, &buf)
	return buf.Bytes(), err
}

func decMetricItem(data []byte) (MetricItem, error) {
	return DecodeMetricItem(bytes.NewReader(data))
}

// EncodeMetricItem serializes a MetricItem using compact binary format.
func EncodeMetricItem(v MetricItem, w io.Writer) error {
	var isDLQ byte
	if v.IsDLQ {
		isDLQ = 1
	}
	if _, err := w.Write([]byte{isDLQ}); err != nil {
		return err
	}
	return writeString(w, v.Origin)
}

// DecodeMetricItem deserializes a MetricItem from r.
func DecodeMetricItem(r io.Reader) (MetricItem, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return MetricItem{}, err
	}
	origin, err := readString(r)
	if err != nil && !errors.Is(err, ErrNilToken) {
		return MetricItem{}, err
	}
	return MetricItem{
		Origin: origin,
		IsDLQ:  b[0] == 1,
	}, nil
}

type MetricsAccumulator struct {
	TotalRead     int64                          `json:"total_read"`
	Valid         int64                          `json:"valid"`
	DLQ           int64                          `json:"dlq"`
	OriginMetrics map[string]domain.OriginMetric `json:"origin_metrics,omitempty"`
}

type MetricsCombinerFn struct{}

func (fn *MetricsCombinerFn) CreateAccumulator() MetricsAccumulator {
	return MetricsAccumulator{
		OriginMetrics: make(map[string]domain.OriginMetric),
	}
}

func (fn *MetricsCombinerFn) AddInput(a MetricsAccumulator, item MetricItem) MetricsAccumulator {
	a.TotalRead++
	if a.OriginMetrics == nil {
		a.OriginMetrics = make(map[string]domain.OriginMetric)
	}

	origin := item.Origin
	if origin == "" {
		origin = "default"
	}

	om := a.OriginMetrics[origin]
	om.TotalRead++
	if item.IsDLQ {
		a.DLQ++
		om.DLQ++
	} else {
		a.Valid++
		om.Valid++
	}
	a.OriginMetrics[origin] = om
	return a
}

func (fn *MetricsCombinerFn) MergeAccumulators(a, b MetricsAccumulator) MetricsAccumulator {
	res := MetricsAccumulator{
		TotalRead:     a.TotalRead + b.TotalRead,
		Valid:         a.Valid + b.Valid,
		DLQ:           a.DLQ + b.DLQ,
		OriginMetrics: make(map[string]domain.OriginMetric, len(a.OriginMetrics)+len(b.OriginMetrics)),
	}
	for k, v := range a.OriginMetrics {
		res.OriginMetrics[k] = v
	}
	for k, vb := range b.OriginMetrics {
		va := res.OriginMetrics[k]
		res.OriginMetrics[k] = domain.OriginMetric{
			TotalRead: va.TotalRead + vb.TotalRead,
			Valid:     va.Valid + vb.Valid,
			DLQ:       va.DLQ + vb.DLQ,
		}
	}
	return res
}

func (fn *MetricsCombinerFn) ExtractOutput(a MetricsAccumulator) domain.PipelineMetrics {
	return domain.PipelineMetrics{
		TotalRecordsRead: a.TotalRead,
		RowsWritten:      a.Valid,
		DeadLetterCount:  a.DLQ,
		OriginMetrics:    a.OriginMetrics,
	}
}

