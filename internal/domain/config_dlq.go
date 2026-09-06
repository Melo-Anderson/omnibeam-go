package domain

type DLQConfig struct {
	Enabled            bool    `json:"enabled"`
	QuarantinePath     string  `json:"quarantine_path"`
	MaxErrorPercentage float64 `json:"max_error_percentage"`
}

type QualityRule struct {
	Type   string   `json:"type"`             // "not_null", "accepted_values", "row_count_min", "row_count_max"
	Column string   `json:"column,omitempty"`
	Values []string `json:"values,omitempty"` // Valid values for accepted_values
	Value  int64    `json:"value,omitempty"`  // Numeric threshold for row_count_min / row_count_max
}

type QualityConfig struct {
	Rules []QualityRule `json:"rules,omitempty"`
}

// SecurityConfig defines operational security and sanitization settings.
// SensitiveFields are field names whose values are redacted in logs, traces, and DLQ payloads.
type SecurityConfig struct {
	SensitiveFields []string `json:"sensitive_fields,omitempty"`
}

// SecretsConfig defines secret manager connection and routing parameters.
type SecretsConfig struct {
	Provider     string            `json:"provider,omitempty"`
	VaultURL     string            `json:"vault_url,omitempty"`
	VaultToken   string            `json:"vault_token,omitempty"`
	GCPProjectID string            `json:"gcp_project_id,omitempty"`
	HostOverride map[string]string `json:"host_override,omitempty"`
}
