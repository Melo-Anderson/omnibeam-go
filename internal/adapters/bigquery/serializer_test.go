package bigquery_test

import (
	"strings"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/bigquery"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestSerializer_SerializeRecordToJSONProto(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "order_id", Type: domain.TypeInt64, Nullable: false},
			{Name: "customer", Type: domain.TypeString, Nullable: false},
			{Name: "amount", Type: domain.TypeDecimal, Scale: 2, Nullable: true},
			{Name: "active", Type: domain.TypeBool, Nullable: false},
			{Name: "created_at", Type: domain.TypeTimestamp, Nullable: false},
		},
	}
	schema.Index()

	rec := domain.NewGenericRecord("orders_schema", len(schema.Fields))
	rec.SetInt64(0, 1001)
	rec.SetString(1, "Alice")
	rec.SetDecimal(2, 4550, 2) // 45.50
	rec.SetBool(3, true)
	rec.SetTimestamp(4, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC))

	data, err := bigquery.SerializeRecordToJSONProto(rec, schema)
	if err != nil {
		t.Fatalf("SerializeRecordToJSONProto failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty byte slice")
	}
	if want := `"order_id":1001`; !containsStr(string(data), want) {
		t.Errorf("expected serialized data to contain %q, got: %s", want, string(data))
	}
}

func containsStr(s, sub string) bool {
	return strings.Contains(s, sub)
}
