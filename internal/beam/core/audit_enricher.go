// package core contains Beam DoFn transformations for the compute pipeline.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package core

import (
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*domain.GenericRecord)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*domain.DeadLetterRecord)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*AuditEnricherFn)(nil)).Elem())
}

// AuditEnricherFn enriches a GenericRecord with pipeline-level audit metadata.
// It always clones AuditFields to preserve Beam's immutability contract across
// parallel 
type AuditEnricherFn struct{}

// NewAuditEnricherFn constructs a new AuditEnricherFn.
func NewAuditEnricherFn() *AuditEnricherFn {
	return &AuditEnricherFn{}
}

// ProcessElement emits a shallow clone of rec with a newly allocated AuditFields
// map, guaranteeing the original record is never mutated after emission.
func (fn *AuditEnricherFn) ProcessElement(rec *domain.GenericRecord, emit func(*domain.GenericRecord)) {
	if rec == nil {
		return
	}

	enriched := &domain.GenericRecord{
		SchemaID:    rec.SchemaID,
		Values:      rec.Values,
		AuditFields: make(map[string]string, len(rec.AuditFields)+1),
	}
	for k, v := range rec.AuditFields {
		enriched.AuditFields[k] = v
	}
	if _, exists := enriched.AuditFields["_ingested_at"]; !exists {
		enriched.AuditFields["_ingested_at"] = time.Now().UTC().Format(time.RFC3339)
	}

	emit(enriched)
}
