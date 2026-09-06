package core

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
		"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestCastAndValidateFn_BeamPipeline_RoutesRecords(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "amount", Type: domain.TypeFloat64, Nullable: false},
		},
	}

	rValid := domain.NewGenericRecord("test.csv", 2)
	rValid.SetString(0, "100")
	rValid.SetString(1, "45.50")

	rInvalid := domain.NewGenericRecord("test.csv", 2)
	rInvalid.SetString(0, "not_an_int")
	rInvalid.SetString(1, "45.50")

	input := beam.Create(s, rValid, rInvalid)

	valid, dlq, audit := beam.ParDo3(s,
		NewCastAndValidateFn(schema, domain.QualityConfig{}),
		input,
	)

	passert.Count(s, valid, "valid records", 1)
	passert.Count(s, dlq, "dlq records", 1)
	passert.Count(s, audit, "audit events", 1)

	if err := ptest.Run(p); err != nil {
		t.Fatalf("ptest.Run failed: %v", err)
	}
}
