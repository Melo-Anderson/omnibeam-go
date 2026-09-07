package fastnum_test

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/pkg/fastnum"
)

func TestParseInt64(t *testing.T) {
	tests := []struct {
		input string
		want  int64
		err   bool
	}{
		{"0", 0, false},
		{"42", 42, false},
		{"-12345", -12345, false},
		{"+999", 999, false},
		{"9223372036854775807", math.MaxInt64, false},
		{"-9223372036854775808", math.MinInt64, false},
		{"  42  ", 42, false},
		{"", 0, true},
		{"   ", 0, true},
		{"+", 0, true},
		{"abc", 0, true},
		{"12a34", 0, true},
		{"-", 0, true},
		{"99999999999999999999", 0, true},  // overflow
		{"-99999999999999999999", 0, true}, // underflow
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := fastnum.ParseInt64(tc.input)
			if (err != nil) != tc.err {
				t.Fatalf("ParseInt64(%q) err = %v, wantErr %v", tc.input, err, tc.err)
			}
			if !tc.err && got != tc.want {
				t.Fatalf("ParseInt64(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseFloat64(t *testing.T) {
	tests := []struct {
		input string
		want  float64
		err   bool
	}{
		{"0.0", 0.0, false},
		{"42.5", 42.5, false},
		{"-123.456", -123.456, false},
		{"+123.456", 123.456, false},
		{"100", 100.0, false},
		{".5", 0.5, false},
		{"-.5", -0.5, false},
		{"  3.14  ", 3.14, false},
		{"", 0, true},
		{"   ", 0, true},
		{"+", 0, true},
		{"-", 0, true},
		{"invalid", 0, true},
		{".", 0, true},
		{"12.34.56", 0, true},
		{"12.34a", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := fastnum.ParseFloat64(tc.input)
			if (err != nil) != tc.err {
				t.Fatalf("ParseFloat64(%q) err = %v, wantErr %v", tc.input, err, tc.err)
			}
			if !tc.err && math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("ParseFloat64(%q) = %f, want %f", tc.input, got, tc.want)
			}
		})
	}
}

func BenchmarkParseInt64_Fastnum(b *testing.B) {
	input := "  123456789  "
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = fastnum.ParseInt64(input)
	}
}

func BenchmarkParseInt64_Strconv(b *testing.B) {
	input := "  123456789  "
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = strconv.ParseInt(strings.TrimSpace(input), 10, 64)
	}
}

func BenchmarkParseFloat64_Fastnum(b *testing.B) {
	input := "  12345.6789  "
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = fastnum.ParseFloat64(input)
	}
}

func BenchmarkParseFloat64_Strconv(b *testing.B) {
	input := "  12345.6789  "
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = strconv.ParseFloat(strings.TrimSpace(input), 64)
	}
}
