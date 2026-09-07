package domain

import (
	"testing"
)

func TestCheckNonNegative_Validation(t *testing.T) {
	tests := []struct {
		name    string
		val     int
		wantErr bool
	}{
		{"zero", 0, false},
		{"positive", 5, false},
		{"negative", -1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckNonNegative("retries", tc.val)
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckNonNegative() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestCheckPositive_Validation(t *testing.T) {
	tests := []struct {
		name    string
		val     int
		wantErr bool
	}{
		{"positive", 100, false},
		{"zero", 0, true},
		{"negative", -10, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckPositive("batch_size", tc.val)
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckPositive() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestCheckNotEmpty_Validation(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"valid", "postgres", false},
		{"empty", "", true},
		{"whitespace", "  ", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckNotEmpty("driver", tc.val)
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckNotEmpty() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestCheckOneOf_Validation(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"valid_csv", "csv", false},
		{"valid_case_insensitive", "CSV", false},
		{"invalid_xml", "xml", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckOneOf("format", tc.val, "csv", "jsonl", "parquet")
			if (err != nil) != tc.wantErr {
				t.Errorf("CheckOneOf() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
