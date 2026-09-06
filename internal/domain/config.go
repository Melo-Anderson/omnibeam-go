// Package domain defines the core data types, entities, value objects,
// and validation rules for the dataflow compute engine.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

type PipelineConfig struct {
	PipelineID     string                `json:"pipeline_id"`
	RunID          string                `json:"run_id"`
	PipelineType   string                `json:"pipeline_type"` // "ingestion", "sql", "api"
	Runner         string                `json:"runner"`        // "direct", "dataflow"
	SecretsConfig  *SecretsConfig        `json:"secrets_config,omitempty"`
	Schema         *Schema               `json:"schema,omitempty"`
	Source         SourceConfig          `json:"source"`
	DatabaseSource *DatabaseSourceConfig `json:"database_source,omitempty"`
	APISource      *APISourceConfig      `json:"api_source,omitempty"`
	Destination    DestinationConfig     `json:"destination"`
	DLQConfig      DLQConfig             `json:"dlq_config"`
	QualityConfig  QualityConfig         `json:"quality_config,omitempty"`
	SecurityConfig SecurityConfig        `json:"security,omitempty"`
}

// GetSchema returns the active schema from the configured source.
func (c *PipelineConfig) GetSchema() Schema {
	if c.Schema != nil && len(c.Schema.Fields) > 0 {
		return *c.Schema
	}
	if c.DatabaseSource != nil && len(c.DatabaseSource.Schema.Fields) > 0 {
		return c.DatabaseSource.Schema
	}
	if c.APISource != nil && len(c.APISource.Schema.Fields) > 0 {
		return c.APISource.Schema
	}
	return c.Source.Schema
}

// RequiresGCS returns true if any configured source, destination, or DLQ URI refers to GCS (gs://).
func (c *PipelineConfig) RequiresGCS() bool {
	if c == nil {
		return false
	}
	if strings.HasPrefix(c.Source.Path, "gs://") || strings.HasPrefix(c.Source.Path, "gs:\\") {
		return true
	}
	for _, p := range c.Source.Paths {
		if strings.HasPrefix(p, "gs://") || strings.HasPrefix(p, "gs:\\") {
			return true
		}
	}
	if strings.HasPrefix(c.Destination.OutputPath, "gs://") || strings.HasPrefix(c.Destination.OutputPath, "gs:\\") {
		return true
	}
	if strings.HasPrefix(c.DLQConfig.QuarantinePath, "gs://") || strings.HasPrefix(c.DLQConfig.QuarantinePath, "gs:\\") {
		return true
	}
	return false
}

// GetMetricsOutputDir returns the output directory for pipeline execution metrics.
// Falls back to Destination.MetricsDir, the directory of Destination.OutputPath, or the DLQ quarantine directory.
func (c *PipelineConfig) GetMetricsOutputDir() string {
	if c == nil {
		return "."
	}
	if strings.TrimSpace(c.Destination.MetricsDir) != "" {
		return strings.TrimSpace(c.Destination.MetricsDir)
	}
	if strings.TrimSpace(c.Destination.OutputPath) != "" {
		out := strings.TrimSpace(c.Destination.OutputPath)
		if filepath.Ext(out) != "" {
			return filepath.Dir(out)
		}
		return out
	}
	if strings.TrimSpace(c.DLQConfig.QuarantinePath) != "" {
		q := strings.TrimSpace(c.DLQConfig.QuarantinePath)
		if filepath.Ext(q) != "" {
			return filepath.Dir(q)
		}
		return q
	}
	return "."
}

// ApplyDefaults populates default configuration values for omitted (zero-value) fields across all components.
func (c *PipelineConfig) ApplyDefaults() {
	c.Runner = OrDefault(c.Runner, DefaultPipelineRunner)
	if c.Schema != nil && len(c.Schema.Fields) > 0 && len(c.Source.Schema.Fields) == 0 {
		c.Source.Schema = *c.Schema
	}
	c.Source.ApplyDefaults()
	if c.DatabaseSource != nil {
		c.DatabaseSource.ApplyDefaults()
	}
	if c.APISource != nil {
		c.APISource.ApplyDefaults()
	}
	c.Destination.ApplyDefaults()
	if c.DLQConfig.QuarantinePath == "" && c.Destination.OutputPath != "" {
		c.DLQConfig.QuarantinePath = filepath.Join(filepath.Dir(c.Destination.OutputPath), "quarantine")
	}
}

// NormalizeAndValidate applies defaults and executes comprehensive validation in a single entrypoint.
func (c *PipelineConfig) NormalizeAndValidate() error {
	c.ApplyDefaults()
	return c.Validate()
}

// Validate checks business rules and required fields without mutating state.
func (c *PipelineConfig) Validate() error {
	if err := ValidateAll(
		CheckNotEmpty("pipeline_id", c.PipelineID),
		CheckNotEmpty("run_id", c.RunID),
	); err != nil {
		return err
	}
	if err := c.validateSourceConfig(); err != nil {
		return err
	}
	if err := c.validateDatabaseConfig(); err != nil {
		return err
	}
	if err := c.validateAPIConfig(); err != nil {
		return err
	}
	return c.validateDestinationConfig()
}

func (c *PipelineConfig) validateSourceConfig() error {
	if err := CheckNonNegative("source.chunk_size_bytes", c.Source.ChunkSizeBytes); err != nil {
		return err
	}
	if c.DatabaseSource == nil && c.APISource == nil && c.Source.Type != "database" && c.Source.Type != "api" {
		if len(c.Source.AllPaths()) == 0 {
			return errors.New("source.path or source.paths is required for file pipelines")
		}
		if len(c.Source.Schema.Fields) == 0 {
			return errors.New("source.schema must define at least one field")
		}
	}
	return nil
}

func (c *PipelineConfig) validateDatabaseConfig() error {
	if c.DatabaseSource == nil {
		return nil
	}
	if err := ValidateAll(
		CheckNonNegative("database_source.partition_config.batch_size", c.DatabaseSource.PartitionConfig.BatchSize),
		CheckNonNegative("database_source.pool_config.max_open_conns", c.DatabaseSource.PoolConfig.MaxOpenConns),
		CheckNonNegative("database_source.pool_config.max_idle_conns", c.DatabaseSource.PoolConfig.MaxIdleConns),
		CheckNotEmpty("database_source.driver", c.DatabaseSource.Driver),
	); err != nil {
		return err
	}
	if strings.TrimSpace(c.DatabaseSource.ConnectionURI) == "" && strings.TrimSpace(c.DatabaseSource.Host) == "" {
		return errors.New("database_source connection_uri or host is required")
	}
	if strings.TrimSpace(c.DatabaseSource.Query) == "" && strings.TrimSpace(c.DatabaseSource.Table) == "" {
		return errors.New("database_source table or query is required")
	}
	if len(c.DatabaseSource.Schema.Fields) == 0 {
		return errors.New("database_source.schema must define at least one field")
	}
	return nil
}

func (c *PipelineConfig) validateAPIConfig() error {
	if c.APISource == nil {
		return nil
	}
	if err := ValidateAll(
		CheckNonNegative("api_source.pagination.page_size", c.APISource.Pagination.PageSize),
		CheckNonNegative("api_source.pagination.max_pages_limit", c.APISource.Pagination.MaxPagesLimit),
		c.APISource.Retry.Validate(),
		CheckNotEmpty("api_source.base_url", c.APISource.BaseURL),
		CheckNotEmpty("api_source.endpoint", c.APISource.Endpoint),
	); err != nil {
		return err
	}
	if len(c.APISource.Schema.Fields) == 0 {
		return errors.New("api_source.schema must define at least one field")
	}
	return nil
}

func (c *PipelineConfig) validateDestinationConfig() error {
	if c.Destination.Type != "rest_api" && c.Destination.Type != "bigquery" && strings.TrimSpace(c.Destination.OutputPath) == "" {
		return errors.New("destination.output_path is required")
	}
	if c.Destination.Type == "rest_api" {
		if err := ValidateAll(
			CheckNotEmpty("destination.endpoint.base_url", c.Destination.Endpoint.BaseURL),
			CheckNonNegative("destination.api_options.batch_size", c.Destination.APIOptions.BatchSize),
		); err != nil {
			return err
		}
	}
	if c.Destination.Type == "bigquery" {
		if err := c.Destination.BigQueryOptions.Validate(); err != nil {
			return fmt.Errorf("destination.bigquery_options: %w", err)
		}
	}
	return nil
}

// ParsePipelineConfig deserializes, applies defaults, and validates a JSON manifest.
func ParsePipelineConfig(jsonBytes []byte) (*PipelineConfig, error) {
	var cfg PipelineConfig
	if err := json.Unmarshal(jsonBytes, &cfg); err != nil {
		return nil, fmt.Errorf("invalid config json: %w", err)
	}
	if err := cfg.NormalizeAndValidate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}
	return &cfg, nil
}
