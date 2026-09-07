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

type DestinationConfig struct {
	Type                string             `json:"type"`          // "local_storage", "storage", "rest_api"
	OutputFormat        string             `json:"output_format"` // "parquet", "csv", "jsonl", "txt"
	OutputPath          string             `json:"output_path"`
	MetricsDir          string             `json:"metrics_dir,omitempty"`
	Compression         string             `json:"compression"` // "snappy", "gzip", "zstd", "none"
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
