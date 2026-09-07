package domain

import "time"

// AuditSeverity represents the criticality level of an audit event.
type AuditSeverity string

const (
	SeverityInfo    AuditSeverity = "INFO"
	SeverityWarning AuditSeverity = "WARNING"
	SeverityError   AuditSeverity = "ERROR"
)

// AuditRecord captures structured telemetry and data-quality events
// emitted during pipeline execution (errors and soft warnings only — not a per-record trace).
type AuditRecord struct {
	Timestamp   time.Time     `json:"timestamp"`
	EventType   string        `json:"event_type"`
	Severity    AuditSeverity `json:"severity"`
	ColumnName  string        `json:"column_name,omitempty"`
	RuleName    string        `json:"rule_name,omitempty"`
	Message     string        `json:"message"`
	SampleValue string        `json:"sample_value,omitempty"`
	PipelineID  string        `json:"pipeline_id,omitempty"`
	RunID       string        `json:"run_id,omitempty"`
}

// NewAuditRecord builds an immutable AuditRecord with a UTC timestamp.
func NewAuditRecord(eventType string, severity AuditSeverity, col, rule, msg, sample string) *AuditRecord {
	return &AuditRecord{
		Timestamp:   time.Now().UTC(),
		EventType:   eventType,
		Severity:    severity,
		ColumnName:  col,
		RuleName:    rule,
		Message:     msg,
		SampleValue: sample,
	}
}
