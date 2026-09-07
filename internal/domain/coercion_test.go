package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestCoerceInt64Fast(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "positive integer", input: "42", want: 42},
		{name: "negative integer", input: "-7", want: -7},
		{name: "spaced integer", input: "  123  ", want: 123},
		{name: "empty string", input: "", wantErr: true},
		{name: "not a number", input: "abc", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := domain.CoerceInt64Fast(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got error=%v, wantErr=%v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCoerceFloat64Fast(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    float64
		wantErr bool
	}{
		{name: "standard float", input: "42.5", want: 42.5},
		{name: "negative float", input: "-10.75", want: -10.75},
		{name: "spaced float", input: "  3.14  ", want: 3.14},
		{name: "empty string", input: "", wantErr: true},
		{name: "invalid", input: "abc", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := domain.CoerceFloat64Fast(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got error=%v, wantErr=%v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("got %f, want %f", got, tc.want)
			}
		})
	}
}

func TestCoerceBoolFast(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    bool
		wantErr bool
	}{
		{name: "true string", input: "true", want: true},
		{name: "1 string", input: "1", want: true},
		{name: "t string", input: "t", want: true},
		{name: "yes string", input: "yes", want: true},
		{name: "y string", input: "y", want: true},
		{name: "false string", input: "false", want: false},
		{name: "0 string", input: "0", want: false},
		{name: "f string", input: "f", want: false},
		{name: "no string", input: "no", want: false},
		{name: "n string", input: "n", want: false},
		{name: "invalid", input: "maybe", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := domain.CoerceBoolFast(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got error=%v, wantErr=%v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCoerceTimestampFast(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "rfc3339", input: "2026-08-23T14:00:00Z", wantErr: false},
		{name: "spaced datetime", input: "2026-08-23 14:00:00", wantErr: false},
		{name: "date only", input: "2026-08-23", wantErr: false},
		{name: "empty string", input: "", wantErr: true},
		{name: "invalid string", input: "invalid-date", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := domain.CoerceTimestampFast(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("got error=%v, wantErr=%v", err, tc.wantErr)
			}
			if err == nil && got.IsZero() {
				t.Errorf("expected non-zero time for %q", tc.input)
			}
		})
	}
}

func TestCoerceDecimalFast_RoundingAndOverflowModes(t *testing.T) {
	tests := []struct {
		input        string
		scale        int32
		onOverflow   string
		wantMantissa int64
		wantScale    int32
		wantErr      bool
	}{
		{"99.995", 2, "round", 10000, 2, false},
		{"99.994", 2, "round", 9999, 2, false},
		{"-99.995", 2, "round", -10000, 2, false},
		{"-99.994", 2, "round", -9999, 2, false},
		{"0.005", 2, "round", 1, 2, false},
		{"0.004", 2, "round", 0, 2, false},
		{".995", 2, "round", 100, 2, false},
		{".5", 2, "round", 50, 2, false},
		{"100", 2, "round", 10000, 2, false},
		{"100.5", 2, "round", 10050, 2, false},
		{"12.3456", 2, "fail", 0, 0, true},
		{"", 2, "round", 0, 2, false},
		{"invalid", 2, "round", 0, 0, true},
		{"1.2.3", 2, "round", 0, 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.input+"_"+tc.onOverflow, func(t *testing.T) {
			m, s, err := domain.CoerceDecimalFast(tc.input, tc.scale, tc.onOverflow)
			if (err != nil) != tc.wantErr {
				t.Fatalf("input %q: error = %v, wantErr = %v", tc.input, err, tc.wantErr)
			}
			if !tc.wantErr {
				if m != tc.wantMantissa || s != tc.wantScale {
					t.Errorf("input %q: got (%d, %d), want (%d, %d)", tc.input, m, s, tc.wantMantissa, tc.wantScale)
				}
			}
		})
	}
}
