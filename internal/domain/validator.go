package domain

import (
	"fmt"
	"strings"
)

// Number represents numeric scalar types suitable for boundary checks.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// CheckNonNegative verifies that a numeric value is greater than or equal to zero.
func CheckNonNegative[T Number](name string, val T) error {
	if val < 0 {
		return fmt.Errorf("%s cannot be negative, got %v", name, val)
	}
	return nil
}

// CheckPositive verifies that a numeric value is strictly greater than zero.
func CheckPositive[T Number](name string, val T) error {
	if val <= 0 {
		return fmt.Errorf("%s must be strictly positive, got %v", name, val)
	}
	return nil
}

// CheckNotEmpty verifies that a string field is not empty or pure whitespace.
func CheckNotEmpty(name, val string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return nil
}

// CheckOneOf verifies that a string value belongs to an allowed set of options.
func CheckOneOf(name, val string, allowed ...string) error {
	trimmed := strings.TrimSpace(val)
	for _, a := range allowed {
		if strings.EqualFold(trimmed, a) {
			return nil
		}
	}
	return fmt.Errorf("%s must be one of %v, got %q", name, allowed, val)
}

// ValidateAll runs a slice of validator errors and returns the first encountered error, or nil.
func ValidateAll(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
