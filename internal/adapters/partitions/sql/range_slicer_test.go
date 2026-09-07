package sql_test

import (
	"testing"

	sql_adapter "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/sql"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestBuildUniversalBoundsQuery(t *testing.T) {
	baseQuery := "SELECT id, nome FROM clientes WHERE id > 5000"
	query := sql_adapter.BuildUniversalBoundsQuery(baseQuery, "id", 1000)

	expected := "SELECT bucket_id, MIN(id) AS lower_bound, MAX(id) AS upper_bound, COUNT(*) AS row_count FROM (SELECT id, CEIL(ROW_NUMBER() OVER (ORDER BY id) / 1000.0) AS bucket_id FROM (SELECT id, nome FROM clientes WHERE id > 5000) AS _user_subq) AS _ranked GROUP BY bucket_id ORDER BY bucket_id"

	if query != expected {
		t.Errorf("bounds query mismatch:\nGot:  %s\nWant: %s", query, expected)
	}
}

func TestBuildWorkerSliceQuery_BoundaryIsolation(t *testing.T) {
	tests := []struct {
		name          string
		baseQuery     string
		col           string
		slice         ports.SQLSlice
		isDollarStyle bool
		wantQuery     string
		wantArgs      []any
	}{
		{
			name:          "nil bounds returns full scan",
			baseQuery:     "SELECT id FROM users",
			col:           "id",
			slice:         ports.SQLSlice{SliceIndex: 0, IsFirst: true, IsLast: true, LowerBound: nil, UpperBound: nil},
			isDollarStyle: true,
			wantQuery:     "SELECT * FROM (SELECT id FROM users) AS _user_subq",
			wantArgs:      nil,
		},
		{
			name:          "single slice (IsFirst && IsLast with bounds) returns full scan",
			baseQuery:     "SELECT id, name FROM users",
			col:           "id",
			slice:         ports.SQLSlice{SliceIndex: 0, IsFirst: true, IsLast: true, LowerBound: 1, UpperBound: 100},
			isDollarStyle: true,
			wantQuery:     "SELECT * FROM (SELECT id, name FROM users) AS _user_subq",
			wantArgs:      nil,
		},
		{
			name:          "intermediate slice uses semi-open interval [lower, upper) dollar style",
			baseQuery:     "SELECT id, name FROM users",
			col:           "id",
			slice:         ports.SQLSlice{SliceIndex: 1, IsFirst: false, IsLast: false, LowerBound: 100, UpperBound: 200},
			isDollarStyle: true,
			wantQuery:     "SELECT * FROM (SELECT id, name FROM users) AS _user_subq WHERE id >= $1 AND id < $2 ORDER BY id",
			wantArgs:      []any{100, 200},
		},
		{
			name:          "intermediate slice uses semi-open interval question mark style",
			baseQuery:     "SELECT id, name FROM users",
			col:           "id",
			slice:         ports.SQLSlice{SliceIndex: 1, IsFirst: false, IsLast: false, LowerBound: 100, UpperBound: 200},
			isDollarStyle: false,
			wantQuery:     "SELECT * FROM (SELECT id, name FROM users) AS _user_subq WHERE id >= ? AND id < ? ORDER BY id",
			wantArgs:      []any{100, 200},
		},
		{
			name:          "last slice uses open-ended boundary >= lower (captures concurrent inserts)",
			baseQuery:     "SELECT id, name FROM users",
			col:           "id",
			slice:         ports.SQLSlice{SliceIndex: 2, IsFirst: false, IsLast: true, LowerBound: 200, UpperBound: 300},
			isDollarStyle: true,
			wantQuery:     "SELECT * FROM (SELECT id, name FROM users) AS _user_subq WHERE id >= $1 ORDER BY id",
			wantArgs:      []any{200},
		},
		{
			name:          "last slice open-ended question mark style",
			baseQuery:     "SELECT id, name FROM users",
			col:           "id",
			slice:         ports.SQLSlice{SliceIndex: 2, IsFirst: false, IsLast: true, LowerBound: 200, UpperBound: 300},
			isDollarStyle: false,
			wantQuery:     "SELECT * FROM (SELECT id, name FROM users) AS _user_subq WHERE id >= ? ORDER BY id",
			wantArgs:      []any{200},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQuery, gotArgs := sql_adapter.BuildWorkerSliceQuery(tt.baseQuery, tt.col, tt.slice, tt.isDollarStyle)
			if gotQuery != tt.wantQuery {
				t.Errorf("query mismatch:\n got:  %s\n want: %s", gotQuery, tt.wantQuery)
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args length mismatch: got %d, want %d (%v)", len(gotArgs), len(tt.wantArgs), gotArgs)
			}
			for i := range gotArgs {
				if gotArgs[i] != tt.wantArgs[i] {
					t.Errorf("arg[%d]: got %v, want %v", i, gotArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

func TestConstructContiguousSlices(t *testing.T) {
	buckets := []sql_adapter.RawBucket{
		{BucketID: 1, Lower: "u1", Upper: "u2500"},
		{BucketID: 2, Lower: "u2501", Upper: "u5000"},
		{BucketID: 3, Lower: "u5001", Upper: "u7500"},
	}

	slices := sql_adapter.ConstructContiguousSlices(buckets)
	if len(slices) != 3 {
		t.Fatalf("expected 3 slices, got %d", len(slices))
	}

	if slices[0].LowerBound != "u1" || slices[0].UpperBound != "u2501" {
		t.Errorf("slice 0 bounds mismatch: [%v, %v)", slices[0].LowerBound, slices[0].UpperBound)
	}
	if !slices[0].IsFirst || slices[0].IsLast {
		t.Errorf("slice 0 flags invalid: %+v", slices[0])
	}

	if slices[2].LowerBound != "u5001" || slices[2].UpperBound != "u7500" {
		t.Errorf("slice 2 bounds mismatch: [%v, %v]", slices[2].LowerBound, slices[2].UpperBound)
	}
	if slices[2].IsFirst || !slices[2].IsLast {
		t.Errorf("slice 2 flags invalid: %+v", slices[2])
	}

	// Empty buckets return single slice
	emptySlices := sql_adapter.ConstructContiguousSlices(nil)
	if len(emptySlices) != 1 || !emptySlices[0].IsFirst || !emptySlices[0].IsLast {
		t.Errorf("expected 1 fallback slice for empty buckets, got %+v", emptySlices)
	}

	// Single bucket returns single slice
	singleSlices := sql_adapter.ConstructContiguousSlices([]sql_adapter.RawBucket{{BucketID: 1, Lower: 1, Upper: 10}})
	if len(singleSlices) != 1 || !singleSlices[0].IsFirst || !singleSlices[0].IsLast {
		t.Errorf("expected 1 slice for single bucket, got %+v", singleSlices)
	}
}
