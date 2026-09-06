package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// EncodeGenericRecord serializes rec into w using compact binary format.
// Format: [schemaID][fieldCount][nullBitmask][fields...][auditFields...]
func EncodeGenericRecord(rec *domain.GenericRecord, w io.Writer) error {
	if rec == nil {
		return writeVarint(w, -1)
	}
	if err := writeString(w, rec.SchemaID); err != nil {
		return err
	}
	n := len(rec.Values)
	if err := writeVarint(w, int64(n)); err != nil {
		return err
	}
	if err := writeNullMask(w, rec.Values); err != nil {
		return err
	}
	if err := writeFieldValues(w, rec.Values); err != nil {
		return err
	}
	return writeAuditFields(w, rec.AuditFields)
}

// DecodeGenericRecord deserializes a GenericRecord from r.
func DecodeGenericRecord(r io.Reader) (*domain.GenericRecord, error) {
	schemaID, err := readString(r)
	if err != nil {
		if errors.Is(err, ErrNilToken) {
			return nil, nil
		}
		return nil, fmt.Errorf("coders: schemaID: %w", err)
	}
	n64, err := readVarint(r)
	if err != nil {
		return nil, fmt.Errorf("coders: fieldCount: %w", err)
	}
	if n64 == -1 {
		return nil, nil
	}
	if n64 < 0 || n64 > domain.DefaultMaxRecordFields {
		return nil, fmt.Errorf("coders: invalid field count %d (max allowed: %d)", n64, domain.DefaultMaxRecordFields)
	}

	n := int(n64)
	var stackMask [domain.DefaultStackMaskBytes]byte
	mask, err := readNullMaskWithBuf(r, n, stackMask[:])
	if err != nil {
		return nil, err
	}
	rec := domain.NewGenericRecord(schemaID, n)
	if err := readFieldValues(r, rec.Values, mask); err != nil {
		return nil, err
	}
	if err := readAuditFields(r, rec.AuditFields); err != nil {
		return nil, err
	}
	return rec, nil
}

// EncodeDeadLetterRecord serializes a DLQ record into w.
func EncodeDeadLetterRecord(dlq *domain.DeadLetterRecord, w io.Writer) error {
	if dlq == nil {
		return writeVarint(w, -1)
	}
	for _, s := range []string{dlq.RawPayload, dlq.ErrorMessage, dlq.FailedColumn, dlq.SourceFile} {
		if err := writeString(w, s); err != nil {
			return err
		}
	}
	return writeVarint(w, dlq.FailedAt.UnixNano())
}

// DecodeDeadLetterRecord deserializes a DLQ record from r.
func DecodeDeadLetterRecord(r io.Reader) (*domain.DeadLetterRecord, error) {
	var dlq domain.DeadLetterRecord
	strs := []*string{&dlq.RawPayload, &dlq.ErrorMessage, &dlq.FailedColumn, &dlq.SourceFile}
	for i, sp := range strs {
		s, err := readString(r)
		if err != nil {
			if i == 0 && errors.Is(err, ErrNilToken) {
				return nil, nil
			}
			return nil, err
		}
		*sp = s
	}
	ns, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	dlq.FailedAt = time.Unix(0, ns).UTC()
	return &dlq, nil
}

// EncodePipelineMetrics serializes a PipelineMetrics into w using compact binary format.
func EncodePipelineMetrics(m domain.PipelineMetrics, w io.Writer) error {
	if err := writeString(w, m.PipelineID); err != nil {
		return err
	}
	if err := writeString(w, m.RunID); err != nil {
		return err
	}
	for _, v := range []int64{
		m.TotalRecordsRead,
		m.RowCount,
		m.RowsWritten,
		m.DeadLetterCount,
		m.BytesWritten,
		m.FilesWritten,
		m.APICallsMade,
		m.ExecutionDurationMs,
	} {
		if err := writeVarint(w, v); err != nil {
			return err
		}
	}
	if err := writeString(w, m.Checksum); err != nil {
		return err
	}
	if err := writeMapStringInt64(w, m.ColumnNullCounts); err != nil {
		return err
	}
	if err := writeMapStringInt64(w, m.InvalidValueCounts); err != nil {
		return err
	}
	return nil
}

func writeMapStringInt64(w io.Writer, m map[string]int64) error {
	if err := writeVarint(w, int64(len(m))); err != nil {
		return err
	}
	for k, v := range m {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := writeVarint(w, v); err != nil {
			return err
		}
	}
	return nil
}

// DecodePipelineMetrics deserializes a PipelineMetrics from r.
func DecodePipelineMetrics(r io.Reader) (domain.PipelineMetrics, error) {
	pipeID, err := readString(r)
	if err != nil {
		return domain.PipelineMetrics{}, err
	}
	runID, err := readString(r)
	if err != nil {
		return domain.PipelineMetrics{}, err
	}
	vals := make([]int64, 8)
	for i := range vals {
		v, err := readVarint(r)
		if err != nil {
			return domain.PipelineMetrics{}, err
		}
		vals[i] = v
	}
	checksum, err := readString(r)
	if err != nil {
		return domain.PipelineMetrics{}, err
	}
	nullCounts, err := readMapStringInt64(r)
	if err != nil {
		return domain.PipelineMetrics{}, err
	}
	invalidCounts, err := readMapStringInt64(r)
	if err != nil {
		return domain.PipelineMetrics{}, err
	}
	return domain.PipelineMetrics{
		PipelineID:          pipeID,
		RunID:               runID,
		TotalRecordsRead:    vals[0],
		RowCount:            vals[1],
		RowsWritten:         vals[2],
		DeadLetterCount:     vals[3],
		BytesWritten:        vals[4],
		FilesWritten:        vals[5],
		APICallsMade:        vals[6],
		ExecutionDurationMs: vals[7],
		Checksum:            checksum,
		ColumnNullCounts:    nullCounts,
		InvalidValueCounts:  invalidCounts,
		CustomMetrics:       make(map[string]any),
	}, nil
}

func readMapStringInt64(r io.Reader) (map[string]int64, error) {
	n, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	m := make(map[string]int64, n)
	for i := int64(0); i < n; i++ {
		k, err := readString(r)
		if err != nil {
			return nil, err
		}
		v, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// Beam coder adapters

func encGenericRecord(v domain.GenericRecord) ([]byte, error) {
	var buf bytes.Buffer
	if err := EncodeGenericRecord(&v, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decGenericRecord(data []byte) (domain.GenericRecord, error) {
	rec, err := DecodeGenericRecord(bytes.NewReader(data))
	if err != nil {
		return domain.GenericRecord{}, err
	}
	return *rec, nil
}

func encDeadLetterRecord(v domain.DeadLetterRecord) ([]byte, error) {
	var buf bytes.Buffer
	if err := EncodeDeadLetterRecord(&v, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decDeadLetterRecord(data []byte) (domain.DeadLetterRecord, error) {
	dlq, err := DecodeDeadLetterRecord(bytes.NewReader(data))
	if err != nil {
		return domain.DeadLetterRecord{}, err
	}
	return *dlq, nil
}
