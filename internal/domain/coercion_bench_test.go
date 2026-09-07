package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func BenchmarkCoerceInt64Fast(b *testing.B) {
	input := "12345678901234"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = domain.CoerceInt64Fast(input)
	}
}

func BenchmarkCoerceDecimalFast(b *testing.B) {
	input := "123456.78"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = domain.CoerceDecimalFast(input, 2, "round")
	}
}
