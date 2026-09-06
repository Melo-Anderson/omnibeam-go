// Package core validates the Apache Beam pipeline DAG construction.
package core

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type fakeStorage struct {
	buf bytes.Buffer
}

func (f *fakeStorage) Open(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.buf.Bytes())), nil
}

func (f *fakeStorage) List(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (f *fakeStorage) Size(_ context.Context, _ string) (int64, error) {
	return int64(f.buf.Len()), nil
}

func (f *fakeStorage) CreateTemp(_ context.Context, finalURI string) (string, io.WriteCloser, error) {
	return "temp-" + finalURI, &nopCloser{&f.buf}, nil
}

func (f *fakeStorage) CommitTemp(_ context.Context, _, _ string) error {
	return nil
}

func (f *fakeStorage) AbortTemp(_ context.Context, _ string) error {
	return nil
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

type mockSourceBuilder struct {
	records []*domain.GenericRecord
}

func (m *mockSourceBuilder) BuildSource(s beam.Scope) beam.PCollection {
	return beam.CreateList(s, m.records)
}

type mockSinkBuilder struct{}

func (m *mockSinkBuilder) BuildSink(s beam.Scope, validRecords beam.PCollection) {}

func TestBuildPipeline_Universal(t *testing.T) {
	p, _ := beam.NewPipelineWithRoot()
	storage := &fakeStorage{}
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}

	source := &mockSourceBuilder{records: []*domain.GenericRecord{}}
	sink := &mockSinkBuilder{}
	dlqSink := &DefaultDLQBeamSink{
		Storage: storage,
		DLQPath: "gs://test-bucket/dlq",
	}
	auditSink := &DefaultAuditBeamSink{
		Storage:  storage,
		AuditDir: "gs://test-bucket/audit",
	}

	metricsCol := BuildPipeline(p, source, sink, dlqSink, auditSink, schema, domain.QualityConfig{})

	if !metricsCol.IsValid() {
		t.Error("expected valid metrics PCollection")
	}
	if p.Root().String() == "" {
		t.Error("pipeline root scope is empty")
	}
}

func TestBuildPipeline_WithoutAudit(t *testing.T) {
	p, _ := beam.NewPipelineWithRoot()
	storage := &fakeStorage{}
	schema := domain.Schema{Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}}}
	source := &mockSourceBuilder{records: []*domain.GenericRecord{}}
	sink := &mockSinkBuilder{}
	dlqSink := &DefaultDLQBeamSink{Storage: storage, DLQPath: "gs://test-bucket/dlq"}

	metricsCol := BuildPipeline(p, source, sink, dlqSink, nil, schema, domain.QualityConfig{})

	if !metricsCol.IsValid() {
		t.Error("expected valid metrics PCollection without audit")
	}
	if p.Root().String() == "" {
		t.Error("pipeline root scope is empty")
	}
}

func TestDefaultAuditBeamSink_NilStorageOrEmptyDir(t *testing.T) {
	p, _ := beam.NewPipelineWithRoot()
	s := p.Root()
	events := beam.CreateList(s, []*domain.AuditRecord{})

	sink1 := &DefaultAuditBeamSink{Storage: nil, AuditDir: "some/path"}
	sink1.BuildAuditSink(s, events) // should not panic

	sink2 := &DefaultAuditBeamSink{Storage: &fakeStorage{}, AuditDir: ""}
	sink2.BuildAuditSink(s, events) // should not panic
}

func TestSliceCoders(t *testing.T) {
	partSlice := ports.PartitionSlice{
		SliceIndex: 1,
		LowerBound: int64(10),
		UpperBound: int64(20),
		IsFirst:    true,
		IsLast:     false,
	}

	encPart, err := encPartitionSlice(partSlice)
	if err != nil {
		t.Fatalf("failed encoding partition slice: %v", err)
	}
	decPart, err := decPartitionSlice(encPart)
	if err != nil {
		t.Fatalf("failed decoding partition slice: %v", err)
	}
	if decPart.SliceIndex != partSlice.SliceIndex || !decPart.IsFirst {
		t.Errorf("decoded partition slice mismatch: %+v", decPart)
	}

	pageSlice := ports.PageSlice{
		PageIndex: 2,
		Cursor:    "cursor-xyz",
		PageSize:  50,
	}
	encPage, err := encPageSlice(pageSlice)
	if err != nil {
		t.Fatalf("failed encoding page slice: %v", err)
	}
	decPage, err := decPageSlice(encPage)
	if err != nil {
		t.Fatalf("failed decoding page slice: %v", err)
	}
	if decPage.PageIndex != pageSlice.PageIndex || decPage.Cursor != "cursor-xyz" {
		t.Errorf("decoded page slice mismatch: %+v", decPage)
	}
}

func TestInit_NoDuplicateCoderPanic(t *testing.T) {
	t.Log("partitions and paged_api init() ran alongside core init() without coder conflict")
}

