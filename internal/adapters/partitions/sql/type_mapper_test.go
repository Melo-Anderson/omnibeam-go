package sql_test

import (
	"database/sql"
	"testing"
	"time"

	sql_adapter "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/sql"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestScanRowToRecord(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "amount", Type: domain.TypeFloat64, Nullable: false},
			{Name: "name", Type: domain.TypeString, Nullable: true},
			{Name: "is_active", Type: domain.TypeBool, Nullable: false},
			{Name: "created_at", Type: domain.TypeTimestamp, Nullable: false},
			{Name: "data_blob", Type: domain.TypeBytes, Nullable: true},
			{Name: "untyped_col", Type: "custom", Nullable: true},
		},
	}

	t.Run("Maps valid scanned row to GenericRecord", func(t *testing.T) {
		holder := sql_adapter.NewScanTargetHolder(schema)
		targets := holder.GetScanTargets()

		now := time.Now().UTC().Truncate(time.Microsecond)
		*(targets[0].(*sql.NullInt64)) = sql.NullInt64{Int64: 42, Valid: true}
		*(targets[1].(*sql.NullFloat64)) = sql.NullFloat64{Float64: 99.95, Valid: true}
		*(targets[2].(*sql.NullString)) = sql.NullString{String: "Alice", Valid: true}
		*(targets[3].(*sql.NullBool)) = sql.NullBool{Bool: true, Valid: true}
		*(targets[4].(*sql.NullTime)) = sql.NullTime{Time: now, Valid: true}
		*(targets[5].(*[]byte)) = []byte{0xDE, 0xAD, 0xBE, 0xEF}
		*(targets[6].(*sql.NullString)) = sql.NullString{String: "untyped-raw", Valid: true}

		rec, err := holder.ExtractRecord(schema)
		if err != nil {
			t.Fatalf("unexpected extract error: %v", err)
		}

		if rec.Values[0].Int64Val() != 42 {
			t.Errorf("expected id 42, got %d", rec.Values[0].Int64Val())
		}
		if rec.Values[1].Float64Val() != 99.95 {
			t.Errorf("expected amount 99.95, got %f", rec.Values[1].Float64Val())
		}
		if rec.Values[2].StringVal() != "Alice" {
			t.Errorf("expected name Alice, got %s", rec.Values[2].StringVal())
		}
		if !rec.Values[3].BoolVal() {
			t.Errorf("expected is_active true")
		}
		if !rec.Values[4].TimeVal().Equal(now) {
			t.Errorf("expected timestamp %v, got %v", now, rec.Values[4].TimeVal())
		}
		if len(rec.Values[5].BytesVal()) != 4 {
			t.Errorf("expected 4 bytes, got %d", len(rec.Values[5].BytesVal()))
		}
	})

	t.Run("Extracts NULL column as IsNull on GenericRecord", func(t *testing.T) {
		holder := sql_adapter.NewScanTargetHolder(schema)
		targets := holder.GetScanTargets()

		*(targets[0].(*sql.NullInt64)) = sql.NullInt64{Valid: false} // id is NULL

		rec, err := holder.ExtractRecord(schema)
		if err != nil {
			t.Fatalf("unexpected extract error: %v", err)
		}
		if !rec.Values[0].IsNull {
			t.Errorf("expected rec.Values[0].IsNull to be true, got false")
		}
	})
}
