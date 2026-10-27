package domain

import (
	"fmt"
	"strings"
)

type PartitionConfig struct {
	PartitionColumn string `json:"partition_column,omitempty"`
	BatchSize       int    `json:"batch_size,omitempty"`
	NumPartitions   int    `json:"num_partitions,omitempty"`
}

// ApplyDefaults populates default values for omitted fields in PartitionConfig.
func (p *PartitionConfig) ApplyDefaults() {
	p.BatchSize = OrDefault(p.BatchSize, DefaultDatabaseBatchSize)
}

type PoolConfig struct {
	MaxOpenConns    int `json:"max_open_conns"`
	MaxIdleConns    int `json:"max_idle_conns"`
	ConnMaxLifetime int `json:"conn_max_lifetime_s"`
}

// ApplyDefaults populates default values for omitted fields in PoolConfig.
func (p *PoolConfig) ApplyDefaults() {
	p.MaxOpenConns = OrDefault(p.MaxOpenConns, DefaultPoolMaxOpenConns)
	p.MaxIdleConns = OrDefault(p.MaxIdleConns, DefaultPoolMaxIdleConns)
	p.ConnMaxLifetime = OrDefault(p.ConnMaxLifetime, DefaultPoolConnLifetimeSeconds)
}

type DatabaseSourceConfig struct {
	Driver          string           `json:"driver"`
	ConnectionURI   string           `json:"connection_uri,omitempty"`
	Host            string           `json:"host,omitempty"`
	Port            int              `json:"port,omitempty"`
	Database        string           `json:"database,omitempty"`
	Username        string           `json:"username,omitempty"`
	PasswordRef     string           `json:"password_ref,omitempty"`
	Query           string           `json:"query,omitempty"`
	Table           string           `json:"table,omitempty"`
	QueryFilter     string           `json:"query_filter,omitempty"`
	Columns         []string         `json:"columns,omitempty"`
	FlattenNested   bool             `json:"flatten_nested,omitempty"`
	PartitionConfig PartitionConfig  `json:"partition_config"`
	PoolConfig      PoolConfig       `json:"pool_config"`
	Resilience      ResilienceConfig `json:"resilience,omitempty"`
	Schema          Schema           `json:"schema"`
	Watermark       *WatermarkConfig `json:"watermark,omitempty"`
}

// BaseQuery returns the base SQL statement to be wrapped or executed.
func (cfg *DatabaseSourceConfig) BaseQuery() string {
	if strings.TrimSpace(cfg.Query) != "" {
		return strings.TrimSpace(cfg.Query)
	}
	cols := "*"
	if len(cfg.Columns) > 0 {
		cols = strings.Join(cfg.Columns, ", ")
	}
	if strings.TrimSpace(cfg.QueryFilter) != "" {
		return fmt.Sprintf("SELECT %s FROM %s WHERE %s", cols, cfg.Table, cfg.QueryFilter)
	}
	return fmt.Sprintf("SELECT %s FROM %s", cols, cfg.Table)
}

// EffectiveQuery returns the SQL query with incremental watermark predicate applied or placeholder replaced.
func (cfg *DatabaseSourceConfig) EffectiveQuery() (string, error) {
	if cfg == nil {
		return "", nil
	}
	base := cfg.BaseQuery()
	hasPlaceholder := strings.Contains(base, ":watermark_value")

	if cfg.Watermark == nil {
		if hasPlaceholder {
			return "", fmt.Errorf("query contains :watermark_value placeholder but no watermark configuration was provided")
		}
		return base, nil
	}

	if err := cfg.Watermark.Validate(); err != nil {
		return "", err
	}

	inc, err := cfg.Watermark.Incoming()
	if err != nil {
		return "", err
	}

	if hasPlaceholder {
		if inc == nil {
			return "", fmt.Errorf("query contains :watermark_value placeholder but watermark value is empty or null")
		}
		return strings.ReplaceAll(base, ":watermark_value", inc.SQLLiteral()), nil
	}

	if inc == nil {
		return base, nil
	}

	return fmt.Sprintf("SELECT * FROM (%s) AS _wm WHERE %s > %s", base, cfg.Watermark.Column, inc.SQLLiteral()), nil
}

// ApplyDefaults populates default values for omitted fields in DatabaseSourceConfig.
func (cfg *DatabaseSourceConfig) ApplyDefaults() {
	if cfg == nil {
		return
	}
	cfg.PartitionConfig.ApplyDefaults()
	cfg.PoolConfig.ApplyDefaults()
	cfg.Resilience.ApplyDefaults()
}
