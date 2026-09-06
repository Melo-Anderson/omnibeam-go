package domain_test

import (
	"errors"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestFieldValidationError_Unwrap(t *testing.T) {
	tests := []struct {
		name      string
		sentinel  error
		fieldErr  *domain.FieldValidationError
		wantMatch bool
	}{
		{
			name:     "unwraps ErrTypeCastFailed",
			sentinel: domain.ErrTypeCastFailed,
			fieldErr: &domain.FieldValidationError{
				Field: "amount",
				Value: "not-a-number",
				Type:  domain.TypeFloat64,
				Err:   domain.ErrTypeCastFailed,
			},
			wantMatch: true,
		},
		{
			name:     "unwraps ErrNullConstraint",
			sentinel: domain.ErrNullConstraint,
			fieldErr: &domain.FieldValidationError{
				Field: "id",
				Value: "",
				Type:  domain.TypeInt64,
				Err:   domain.ErrNullConstraint,
			},
			wantMatch: true,
		},
		{
			name:     "does not match wrong sentinel",
			sentinel: domain.ErrSchemaMismatch,
			fieldErr: &domain.FieldValidationError{
				Field: "id",
				Value: "",
				Type:  domain.TypeInt64,
				Err:   domain.ErrNullConstraint,
			},
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errors.Is(tt.fieldErr, tt.sentinel)
			if got != tt.wantMatch {
				t.Errorf("errors.Is(%v, %v) = %v, want %v", tt.fieldErr, tt.sentinel, got, tt.wantMatch)
			}
			if tt.fieldErr.Error() == "" {
				t.Error("FieldValidationError.Error() must not return empty string")
			}
		})
	}
}
