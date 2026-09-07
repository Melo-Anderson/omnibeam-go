package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"pgregory.net/rapid"
)

func TestSchema_FieldByName_RoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 10).Draw(t, "numFields")
		fields := make([]domain.Field, n)
		names := make(map[string]bool)
		for i := 0; i < n; i++ {
			name := rapid.StringMatching(`[a-z][a-z0-9_]{0,15}`).Filter(func(s string) bool {
				return !names[s]
			}).Draw(t, "fieldName")
			names[name] = true
			fields[i] = domain.Field{Name: name, Type: domain.TypeString}
		}
		schema := domain.Schema{Fields: fields}
		schema.Index()

		for i, f := range fields {
			got, idx, ok := schema.FieldByName(f.Name)
			if !ok {
				t.Fatalf("FieldByName(%q) returned false after Index()", f.Name)
			}
			if got.Name != f.Name {
				t.Fatalf("FieldByName(%q) returned field %q", f.Name, got.Name)
			}
			if idx != i {
				t.Fatalf("FieldByName(%q) returned index %d, want %d", f.Name, idx, i)
			}
		}
	})
}
