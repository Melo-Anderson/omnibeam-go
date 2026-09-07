package core

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestAuditEnricherFn_ProcessElement_CloneIsolation(t *testing.T) {
	tests := []struct {
		name              string
		existingAuditKeys map[string]string
		wantIngestedAt    bool
	}{
		{
			name:              "adds _ingested_at to empty AuditFields without mutating original",
			existingAuditKeys: map[string]string{},
			wantIngestedAt:    true,
		},
		{
			name:              "preserves existing keys and adds _ingested_at",
			existingAuditKeys: map[string]string{"_source_file": "data.csv", "_custom": "val"},
			wantIngestedAt:    true,
		},
		{
			name:              "does not overwrite existing _ingested_at",
			existingAuditKeys: map[string]string{"_ingested_at": "2024-01-01T00:00:00Z"},
			wantIngestedAt:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := NewAuditEnricherFn()

			original := domain.NewGenericRecord("test_file.csv", 2)
			for k, v := range tt.existingAuditKeys {
				original.AuditFields[k] = v
			}
			originalMapAddr := &original.AuditFields

			var emitted *domain.GenericRecord
			fn.ProcessElement(original, func(r *domain.GenericRecord) {
				emitted = r
			})

			if emitted == nil {
				t.Fatal("expected emitted record, got nil")
			}
			if emitted == original {
				t.Fatal("expected a cloned record instance, got the same pointer")
			}
			if &emitted.AuditFields == originalMapAddr {
				t.Fatal("emitted record shares the same AuditFields map — not cloned")
			}
			if emitted.AuditFields["_ingested_at"] == "" && tt.wantIngestedAt {
				t.Error("_ingested_at was not set on emitted record")
			}
			if _, mutated := original.AuditFields["_ingested_at"]; mutated {
				if _, hadBefore := tt.existingAuditKeys["_ingested_at"]; !hadBefore {
					t.Fatal("original record AuditFields map was mutated in place")
				}
			}
			for k, v := range tt.existingAuditKeys {
				if got := emitted.AuditFields[k]; got != v {
					t.Errorf("key %q not copied: got %q, want %q", k, got, v)
				}
			}
		})
	}
}
