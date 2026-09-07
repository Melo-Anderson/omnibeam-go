package telemetry_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

func TestSanitizer_SanitizeKeyAndValue(t *testing.T) {
	tests := []struct {
		key      string
		val      any
		custom   []string
		expected string
	}{
		{"password", "super_secret_123", nil, "[REDACTED]"},
		{"api_token", "bearer eyJhbGciOi...", nil, "[REDACTED]"},
		{"client_secret", "sec_xyz", nil, "[REDACTED]"},
		{"auth_header", "Basic dXNlcjpwYXNz", nil, "[REDACTED]"},
		{"user_cpf", "123.456.789-00", []string{"user_cpf", "email"}, "[REDACTED]"},
		{"customer_name", "Acme Corp", nil, "Acme Corp"},
		{"order_id", int64(9999), nil, "9999"},
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			got := telemetry.SanitizeValue(tc.key, tc.val, tc.custom)
			gotStr := ""
			if str, ok := got.(string); ok {
				gotStr = str
			} else if tc.key == "order_id" {
				if v, ok := got.(int64); ok && v == 9999 {
					return
				}
			}
			if gotStr != tc.expected {
				t.Errorf("SanitizeValue(%q, %v) = %v, want %v", tc.key, tc.val, got, tc.expected)
			}
		})
	}
}

func TestSanitizer_SanitizeStringPayload(t *testing.T) {
	raw := `{"password":"secret123","user":"john","token":"xyz987"}`
	sanitized := telemetry.SanitizePayload(raw, nil)
	if !telemetry.ContainsRedacted(sanitized) {
		t.Errorf("expected payload to be sanitized, got: %s", sanitized)
	}
}
