package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors for domain-level validation failures.
// Use errors.Is to match wrapped instances.
var (
	ErrTypeCastFailed = errors.New("type cast failed")
	ErrNullConstraint = errors.New("non-nullable constraint violation")
	ErrSchemaMismatch = errors.New("schema field count mismatch")
)

// FieldValidationError carries structured context about a field-level validation failure.
// It wraps a sentinel error so callers can use errors.Is for programmatic handling.
type FieldValidationError struct {
	Field string
	Value string
	Type  DataType
	Err   error
}

func (e *FieldValidationError) Error() string {
	return fmt.Sprintf(
		"field %q (%s) with value %q: %v",
		e.Field, e.Type, e.Value, e.Err,
	)
}

func (e *FieldValidationError) Unwrap() error {
	return e.Err
}
