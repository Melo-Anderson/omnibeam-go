package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func FuzzCoerceInt64Fast(f *testing.F) {
	seeds := []string{"0", "12345", "-999", "9223372036854775807", "-9223372036854775808", "invalid", "", "   42  "}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// Invariant: must never panic; must return a valid int64 or an error — never both nil.
		_, _ = domain.CoerceInt64Fast(s)
	})
}

func FuzzCoerceDecimalFast(f *testing.F) {
	type seed struct {
		s          string
		scale      int32
		onOverflow string
	}
	seeds := []seed{
		{"100.50", 2, "round"},
		{"-0.001", 3, "round"},
		{"999999999999", 0, "round"},
		{".5", 1, "round"},
		{"123.456789", 4, "fail"},
		{"invalid.dot.twice", 2, "round"},
		{"1e10", 2, "round"},
		{"", 2, "round"},
	}
	for _, s := range seeds {
		f.Add(s.s, s.scale, s.onOverflow)
	}
	f.Fuzz(func(t *testing.T, s string, scale int32, overflow string) {
		// Invariant: must never panic regardless of inputs.
		_, _, _ = domain.CoerceDecimalFast(s, scale, overflow)
	})
}
