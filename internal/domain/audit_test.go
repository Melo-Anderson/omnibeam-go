package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestNewAuditRecord_Fields(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		severity  domain.AuditSeverity
		col       string
		rule      string
		msg       string
		sample    string
	}{
		{
			name:      "cast failure event",
			eventType: "type_cast_failure",
			severity:  domain.SeverityError,
			col:       "age",
			rule:      "type_cast",
			msg:       `cannot cast "abc" to int64`,
			sample:    "abc",
		},
		{
			name:      "quality warning event",
			eventType: "quality_warning",
			severity:  domain.SeverityWarning,
			col:       "name",
			rule:      "not_null",
			msg:       "soft warning",
			sample:    "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := domain.NewAuditRecord(tc.eventType, tc.severity, tc.col, tc.rule, tc.msg, tc.sample)
			if rec.EventType != tc.eventType {
				t.Errorf("EventType: got %q, want %q", rec.EventType, tc.eventType)
			}
			if rec.Severity != tc.severity {
				t.Errorf("Severity: got %q, want %q", rec.Severity, tc.severity)
			}
			if rec.ColumnName != tc.col {
				t.Errorf("ColumnName: got %q, want %q", rec.ColumnName, tc.col)
			}
			if rec.RuleName != tc.rule {
				t.Errorf("RuleName: got %q, want %q", rec.RuleName, tc.rule)
			}
			if rec.Message != tc.msg {
				t.Errorf("Message: got %q, want %q", rec.Message, tc.msg)
			}
			if rec.SampleValue != tc.sample {
				t.Errorf("SampleValue: got %q, want %q", rec.SampleValue, tc.sample)
			}
			if rec.Timestamp.IsZero() {
				t.Error("Timestamp must be set")
			}
		})
	}
}

func TestNewAuditRecord_JSONRoundtrip(t *testing.T) {
	tests := []struct {
		name         string
		rec          *domain.AuditRecord
		wantContains []string
		wantAbsent   []string
	}{
		{
			name: "error event serializes correctly",
			rec:  domain.NewAuditRecord("type_cast_failure", domain.SeverityError, "id", "type_cast", "cannot cast", "abc"),
			wantContains: []string{
				`"event_type":"type_cast_failure"`,
				`"severity":"ERROR"`,
				`"sample_value":"abc"`,
			},
		},
		{
			name: "empty sample_value is omitted",
			rec:  domain.NewAuditRecord("quality_warning", domain.SeverityWarning, "name", "not_null", "soft warning", ""),
			wantContains: []string{
				`"severity":"WARNING"`,
			},
			wantAbsent: []string{
				"sample_value",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.rec)
			if err != nil {
				t.Fatalf("json.Marshal: %v", err)
			}
			jsonStr := string(data)
			for _, want := range tc.wantContains {
				if !strings.Contains(jsonStr, want) {
					t.Errorf("JSON missing %q: %s", want, jsonStr)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(jsonStr, absent) {
					t.Errorf("JSON should not contain %q: %s", absent, jsonStr)
				}
			}
		})
	}
}
