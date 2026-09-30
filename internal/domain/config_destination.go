package domain

import (
	"errors"
	"fmt"
	"strings"
)

type FormatOptions struct {
	Delimiter      string `json:"delimiter,omitempty"`       // ",", ";", "|", "\t"
	IncludeHeader  bool   `json:"include_header,omitempty"`  // true/false
	Charset        string `json:"charset,omitempty"`         // "utf-8", "iso-8859-1"
	LineTerminator string `json:"line_terminator,omitempty"` // "\n", "\r\n"
	QuoteAll       bool   `json:"quote_all,omitempty"`
}

// ApplyDefaults populates default values for omitted fields in FormatOptions.
func (opts *FormatOptions) ApplyDefaults() {
	opts.Delimiter = OrDefault(opts.Delimiter, DefaultDestinationDelimiter)
	opts.LineTerminator = OrDefault(opts.LineTerminator, DefaultDestinationLineTerminator)
	opts.Charset = OrDefault(opts.Charset, DefaultDestinationCharset)
}

// EncryptionConfig defines output-side encryption applied after compression.
// The resulting file extension is appended after the compression extension:
// e.g., data.csv.gz → data.csv.gz.pgp
type EncryptionConfig struct {
	// Type selects the encryption mode. Values: "none" (default), "pgp", "kms".
	Type string `json:"type,omitempty"`

	// PublicKeyRef is the secret reference resolved by SecretResolver to obtain
	// the recipient's PGP public key (armored ASCII or binary).
	// Required when Type = "pgp".
	PublicKeyRef string `json:"public_key_ref,omitempty"`

	// KMSKeyRef is the GCP KMS key resource name used for envelope encryption.
	// Required when Type = "kms".
	KMSKeyRef string `json:"kms_key_ref,omitempty"`
}

// ApplyDefaults sets Type to "none" when not specified.
func (e *EncryptionConfig) ApplyDefaults() {
	e.Type = OrDefault(strings.ToLower(strings.TrimSpace(e.Type)), "none")
}

// Validate checks that required fields are present for the selected encryption mode.
func (e *EncryptionConfig) Validate() error {
	switch e.Type {
	case "none", "":
		return nil
	case "pgp":
		if strings.TrimSpace(e.PublicKeyRef) == "" {
			return errors.New("encryption.public_key_ref is required for pgp encryption")
		}
		return nil
	case "kms":
		if strings.TrimSpace(e.KMSKeyRef) == "" {
			return errors.New("encryption.kms_key_ref is required for kms encryption")
		}
		return nil
	default:
		return fmt.Errorf("unsupported encryption type %q (supported: none, pgp, kms)", e.Type)
	}
}

type DestinationConfig struct {
	Type                string             `json:"type"`          // "local_storage", "storage", "rest_api"
	OutputFormat        string             `json:"output_format"` // "parquet", "csv", "jsonl", "txt"
	OutputPath          string             `json:"output_path"`
	MetricsDir          string             `json:"metrics_dir,omitempty"`
	Compression         string             `json:"compression"` // "snappy", "gzip", "zstd", "none"
	Encryption          EncryptionConfig   `json:"encryption,omitempty"`
	SingleFile          bool               `json:"single_file,omitempty"`
	IncludeAuditColumns bool               `json:"include_audit_columns"`
	FormatOptions       FormatOptions      `json:"format_options,omitempty"`
	Endpoint            APIEndpointConfig  `json:"endpoint,omitempty"`
	APIOptions          APISinkOptions     `json:"api_options,omitempty"`
	BigQueryOptions     BigQuerySinkConfig `json:"bigquery_options,omitempty"`
}

// ApplyDefaults populates default values for omitted fields in DestinationConfig.
func (d *DestinationConfig) ApplyDefaults() {
	d.Type = strings.ToLower(strings.TrimSpace(d.Type))
	d.Type = OrDefault(d.Type, DefaultDestinationType)
	d.OutputFormat = strings.ToLower(strings.TrimSpace(d.OutputFormat))
	d.OutputFormat = OrDefault(d.OutputFormat, d.Type)

	d.APIOptions.ApplyDefaults()
	d.FormatOptions.ApplyDefaults()
	d.Encryption.ApplyDefaults()
}

// BigQuerySinkConfig defines configuration for writing to BigQuery via Storage Write API.
type BigQuerySinkConfig struct {
	ProjectID        string `json:"project_id,omitempty"`
	DatasetID        string `json:"dataset_id"`
	TableID          string `json:"table_id"`
	WriteDisposition string `json:"write_disposition,omitempty"` // "WRITE_APPEND" (default), "WRITE_TRUNCATE", "WRITE_EMPTY"
	BatchSize        int    `json:"batch_size,omitempty"`        // rows per pending-stream flush batch (default: 500)
}

// Validate checks BigQuerySinkConfig fields and applies defaults for omitted optional values.
func (c *BigQuerySinkConfig) Validate() error {
	if strings.TrimSpace(c.DatasetID) == "" {
		return errors.New("bigquery sink requires dataset_id")
	}
	if strings.TrimSpace(c.TableID) == "" {
		return errors.New("bigquery sink requires table_id")
	}
	c.WriteDisposition = OrDefault(c.WriteDisposition, "WRITE_APPEND")
	switch c.WriteDisposition {
	case "WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY":
	default:
		return fmt.Errorf("invalid write_disposition %q (must be WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY)", c.WriteDisposition)
	}
	c.BatchSize = OrDefault(c.BatchSize, DefaultBigQueryBatchSize)
	return nil
}
