package domain_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestParsePipelineConfig(t *testing.T) {
	validJSON := []byte(`{
		"pipeline_id": "pipe-123",
		"run_id": "run-456",
		"pipeline_type": "ingestion",
		"runner": "direct",
		"source": {
			"type": "file",
			"path": "testdata/fixtures/sample.csv",
			"format": "csv",
			"delimiter": ",",
			"charset": "utf-8",
			"compression": "none",
			"schema": {
				"fields": [
					{"name": "id", "type": "int64", "nullable": false},
					{"name": "name", "type": "string", "nullable": true}
				]
			}
		},
		"destination": {
			"type": "local_storage",
			"output_format": "parquet",
			"output_path": "testdata/output/",
			"compression": "snappy"
		},
		"dlq_config": {
			"enabled": true,
			"quarantine_path": "testdata/output/quarantine/"
		}
	}`)

	t.Run("Valid manifest parses correctly", func(t *testing.T) {
		cfg, err := domain.ParsePipelineConfig(validJSON)
		if err != nil {
			t.Fatalf("unexpected error parsing valid config: %v", err)
		}
		if cfg.PipelineID != "pipe-123" {
			t.Errorf("expected pipeline_id 'pipe-123', got %s", cfg.PipelineID)
		}
		if cfg.Source.Format != "csv" {
			t.Errorf("expected source format 'csv', got %s", cfg.Source.Format)
		}
		if len(cfg.Source.Schema.Fields) != 2 {
			t.Errorf("expected 2 schema fields, got %d", len(cfg.Source.Schema.Fields))
		}
	})

	t.Run("Invalid manifest without required fields fails", func(t *testing.T) {
		invalidJSON := []byte(`{"pipeline_id": ""}`)
		_, err := domain.ParsePipelineConfig(invalidJSON)
		if err == nil {
			t.Errorf("expected error for missing required fields, got nil")
		}
	})
}

func TestDatabaseSourceConfig_MongoDB_Validation(t *testing.T) {
	t.Run("Valid MongoDB configuration parses correctly", func(t *testing.T) {
		rawJSON := []byte(`{
			"pipeline_id": "mongo-ingest-01",
			"run_id": "run-01",
			"pipeline_type": "database",
			"database_source": {
				"driver": "mongodb",
				"connection_uri": "mongodb://localhost:27017/analytics",
				"table": "customers",
				"flatten_nested": true,
				"partition_config": {
					"num_partitions": 4,
					"batch_size": 1000
				},
				"schema": {
					"fields": [
						{"name": "id", "type": "string", "nullable": false},
						{"name": "name", "type": "string", "nullable": true},
						{"name": "user.address.city", "type": "string", "nullable": true}
					]
				}
			},
			"destination": {
				"type": "local_storage",
				"output_format": "parquet",
				"output_path": "testdata/output/mongo/"
			}
		}`)

		cfg, err := domain.ParsePipelineConfig(rawJSON)
		if err != nil {
			t.Fatalf("unexpected error parsing valid mongo config: %v", err)
		}
		if cfg.DatabaseSource.Driver != "mongodb" {
			t.Errorf("expected driver mongodb, got: %s", cfg.DatabaseSource.Driver)
		}
		if !cfg.DatabaseSource.FlattenNested {
			t.Errorf("expected FlattenNested to be true")
		}
		if cfg.DatabaseSource.Table != "customers" {
			t.Errorf("expected collection 'customers', got: %s", cfg.DatabaseSource.Table)
		}
	})

	t.Run("MongoDB missing table (collection) fails validation", func(t *testing.T) {
		rawJSON := []byte(`{
			"pipeline_id": "mongo-err",
			"run_id": "run-01",
			"pipeline_type": "database",
			"database_source": {
				"driver": "mongodb",
				"connection_uri": "mongodb://localhost:27017/analytics",
				"schema": {
					"fields": [{"name": "id", "type": "string"}]
				}
			},
			"destination": {
				"output_path": "testdata/output/mongo/"
			}
		}`)

		_, err := domain.ParsePipelineConfig(rawJSON)
		if err == nil {
			t.Fatalf("expected error for missing table/collection in mongo config, got nil")
		}
	})
}

func TestParsePipelineConfig_ExportDestination(t *testing.T) {
	t.Run("Valid File Export Config", func(t *testing.T) {
		rawJSON := []byte(`{
			"pipeline_id": "exp-file-01",
			"run_id": "run-01",
			"pipeline_type": "export",
			"source": {
				"type": "database",
				"driver": "postgres",
				"connection_uri": "postgres://user:pass@host:5432/db",
				"query": "SELECT id, name FROM users",
				"schema": {"fields": [{"name": "id", "type": "int64"}, {"name": "name", "type": "string"}]}
			},
			"destination": {
				"type": "storage",
				"output_path": "/tmp/exports/users.csv.gz",
				"output_format": "csv",
				"single_file": true,
				"format_options": {
					"delimiter": ";",
					"include_header": true,
					"charset": "utf-8",
					"line_terminator": "\r\n"
				},
				"compression": "gzip"
			}
		}`)

		cfg, err := domain.ParsePipelineConfig(rawJSON)
		if err != nil {
			t.Fatalf("unexpected error parsing export config: %v", err)
		}

		if cfg.Destination.OutputFormat != "csv" {
			t.Errorf("expected format csv, got %s", cfg.Destination.OutputFormat)
		}
		if !cfg.Destination.SingleFile {
			t.Errorf("expected SingleFile=true, got false")
		}
		if cfg.Destination.FormatOptions.Delimiter != ";" {
			t.Errorf("expected delimiter ';', got %s", cfg.Destination.FormatOptions.Delimiter)
		}
	})

	t.Run("Valid REST API Export Config", func(t *testing.T) {
		rawJSON := []byte(`{
			"pipeline_id": "exp-api-01",
			"run_id": "run-01",
			"pipeline_type": "export",
			"source": {
				"type": "database",
				"driver": "postgres",
				"connection_uri": "postgres://user:pass@host:5432/db",
				"query": "SELECT id, name FROM users",
				"schema": {"fields": [{"name": "id", "type": "int64"}, {"name": "name", "type": "string"}]}
			},
			"destination": {
				"type": "rest_api",
				"endpoint": {
					"base_url": "https://api.salesforce.com",
					"credential_ref": "vault:secret/salesforce",
					"auth_type": "bearer_token"
				},
				"api_options": {
					"resource_path": "/services/data/v60.0/sobjects/Lead",
					"method": "POST",
					"batch_size": 200,
					"body_envelope": "records",
					"rate_limit_rps": 25
				}
			}
		}`)

		cfg, err := domain.ParsePipelineConfig(rawJSON)
		if err != nil {
			t.Fatalf("unexpected error parsing api export config: %v", err)
		}

		if cfg.Destination.APIOptions.BatchSize != 200 {
			t.Errorf("expected BatchSize 200, got %d", cfg.Destination.APIOptions.BatchSize)
		}
		if cfg.Destination.APIOptions.BodyEnvelope != "records" {
			t.Errorf("expected BodyEnvelope 'records', got %s", cfg.Destination.APIOptions.BodyEnvelope)
		}
	})
}

func TestParsePipelineConfig_SQLSource(t *testing.T) {
	t.Run("Valid PostgreSQL SQL manifest parses and sets defaults", func(t *testing.T) {
		manifestJSON := []byte(`{
			"pipeline_id": "sql-pipe-01",
			"run_id": "run-001",
			"pipeline_type": "sql",
			"database_source": {
				"driver": "postgres",
				"connection_uri": "postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable",
				"table": "public.orders",
				"partition_config": {
					"partition_column": "id"
				},
				"schema": {
					"fields": [
						{"name": "id", "type": "int64", "nullable": false},
						{"name": "customer_id", "type": "string", "nullable": true}
					]
				}
			},
			"destination": {
				"type": "local_storage",
				"output_format": "parquet",
				"output_path": "output/orders.parquet"
			},
			"dlq_config": {
				"enabled": true,
				"quarantine_path": "output/dlq.jsonl"
			}
		}`)

		cfg, err := domain.ParsePipelineConfig(manifestJSON)
		if err != nil {
			t.Fatalf("expected valid config, got: %v", err)
		}

		if cfg.DatabaseSource == nil {
			t.Fatal("expected DatabaseSource to be non-nil")
		}
		if cfg.DatabaseSource.Driver != "postgres" {
			t.Errorf("expected driver postgres, got: %s", cfg.DatabaseSource.Driver)
		}
		if cfg.DatabaseSource.PoolConfig.MaxOpenConns != 4 {
			t.Errorf("expected default MaxOpenConns 4, got: %d", cfg.DatabaseSource.PoolConfig.MaxOpenConns)
		}
		if cfg.DatabaseSource.PoolConfig.MaxIdleConns != 2 {
			t.Errorf("expected default MaxIdleConns 2, got: %d", cfg.DatabaseSource.PoolConfig.MaxIdleConns)
		}
		if cfg.DatabaseSource.PoolConfig.ConnMaxLifetime != 300 {
			t.Errorf("expected default ConnMaxLifetime 300, got: %d", cfg.DatabaseSource.PoolConfig.ConnMaxLifetime)
		}
		if cfg.DatabaseSource.PartitionConfig.BatchSize != 2500 {
			t.Errorf("expected default BatchSize 2500, got: %d", cfg.DatabaseSource.PartitionConfig.BatchSize)
		}
	})

	t.Run("Invalid SQL manifest missing driver or query/table fails validation", func(t *testing.T) {
		manifestJSON := []byte(`{
			"pipeline_id": "sql-pipe-err",
			"run_id": "run-001",
			"pipeline_type": "sql",
			"database_source": {
				"driver": "",
				"schema": {
					"fields": [{"name": "id", "type": "int64"}]
				}
			},
			"destination": {"output_path": "output/out.parquet"}
		}`)

		_, err := domain.ParsePipelineConfig(manifestJSON)
		if err == nil {
			t.Fatal("expected validation error for empty driver/table, got nil")
		}
	})
}

func TestDatabaseSourceConfig_BaseQuery(t *testing.T) {
	t.Run("Returns custom query directly when provided", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{
			Query: "SELECT id, nome FROM clientes WHERE id > 5000",
		}
		if cfg.BaseQuery() != "SELECT id, nome FROM clientes WHERE id > 5000" {
			t.Errorf("expected custom query, got: %s", cfg.BaseQuery())
		}
	})

	t.Run("Constructs base query from table, columns, and filter when query is empty", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{
			Table:       "public.orders",
			QueryFilter: "status = 'ACTIVE'",
			Columns:     []string{"id", "amount"},
		}
		expected := "SELECT id, amount FROM public.orders WHERE status = 'ACTIVE'"
		if cfg.BaseQuery() != expected {
			t.Errorf("expected %q, got %q", expected, cfg.BaseQuery())
		}
	})

	t.Run("Constructs base query from table without filter or columns", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{
			Table: "public.orders",
		}
		expected := "SELECT * FROM public.orders"
		if cfg.BaseQuery() != expected {
			t.Errorf("expected %q, got %q", expected, cfg.BaseQuery())
		}
	})
}

func TestParsePipelineConfig_SecretsConfig(t *testing.T) {
	t.Run("Parses manifest with complete secrets_config block", func(t *testing.T) {
		manifestJSON := `{
			"pipeline_id": "test-sec-01",
			"run_id": "run-001",
			"pipeline_type": "sql",
			"secrets_config": {
				"provider": "openbao",
				"vault_url": "http://openbao:8200",
				"vault_token": "root",
				"gcp_project_id": "my-gcp-project",
				"host_override": {
					"postgres": "localhost",
					"mongodb": "127.0.0.1"
				}
			},
			"database_source": {
				"driver": "postgres",
				"connection_uri": "postgres://postgres:root@postgres:5432/users?sslmode=disable",
				"table": "users",
				"password_ref": "secret/postgres#password",
				"schema": {
					"fields": [{"name": "id", "type": "int64"}]
				}
			},
			"destination": {
				"type": "local_storage",
				"output_format": "parquet",
				"output_path": "./testdata/output/test.parquet"
			}
		}`

		cfg, err := domain.ParsePipelineConfig([]byte(manifestJSON))
		if err != nil {
			t.Fatalf("unexpected error parsing config: %v", err)
		}
		if cfg.SecretsConfig == nil {
			t.Fatal("expected secrets_config to be populated, got nil")
		}
		if cfg.SecretsConfig.Provider != "openbao" {
			t.Errorf("expected provider openbao, got %s", cfg.SecretsConfig.Provider)
		}
		if cfg.SecretsConfig.VaultURL != "http://openbao:8200" {
			t.Errorf("expected vault_url http://openbao:8200, got %s", cfg.SecretsConfig.VaultURL)
		}
		if cfg.SecretsConfig.HostOverride["postgres"] != "localhost" {
			t.Errorf("expected host override for postgres, got %s", cfg.SecretsConfig.HostOverride["postgres"])
		}
	})
}

func TestPipelineConfig_APISourceValidation(t *testing.T) {
	t.Run("Valid api_source config with page_number pagination", func(t *testing.T) {
		rawJSON := []byte(`{
			"pipeline_id": "pipe-rest-01",
			"run_id": "run-01",
			"pipeline_type": "ingestion",
			"runner": "direct",
			"api_source": {
				"base_url": "https://api.example.com",
				"endpoint": "/v1/users",
				"pagination": {
					"type": "page_number",
					"page_size": 50
				},
				"schema": {
					"fields": [
						{"name": "id", "type": "int64", "nullable": false},
						{"name": "name", "type": "string", "nullable": true}
					]
				}
			},
			"destination": {
				"type": "local_storage",
				"output_format": "parquet",
				"output_path": "/tmp/out.parquet"
			},
			"dlq_config": {
				"enabled": true,
				"quarantine_path": "/tmp/dlq"
			}
		}`)

		cfg, err := domain.ParsePipelineConfig(rawJSON)
		if err != nil {
			t.Fatalf("expected valid config, got error: %v", err)
		}
		if cfg.APISource == nil {
			t.Fatal("expected APISource to be populated")
		}
		if cfg.APISource.HTTPMethod != "GET" {
			t.Errorf("expected default HTTPMethod 'GET', got %s", cfg.APISource.HTTPMethod)
		}
		if cfg.APISource.Pagination.PageParam != "page" {
			t.Errorf("expected default PageParam 'page', got %s", cfg.APISource.Pagination.PageParam)
		}
		if cfg.APISource.Pagination.SizeParam != "size" {
			t.Errorf("expected default SizeParam 'size', got %s", cfg.APISource.Pagination.SizeParam)
		}
		if cfg.APISource.Pagination.InitialPage != 1 {
			t.Errorf("expected default InitialPage 1, got %d", cfg.APISource.Pagination.InitialPage)
		}
		if cfg.APISource.Retry.MaxRetries != 3 {
			t.Errorf("expected default MaxRetries 3, got %d", cfg.APISource.Retry.MaxRetries)
		}
		if cfg.APISource.Retry.TimeoutMs != 30000 {
			t.Errorf("expected default TimeoutMs 30000, got %d", cfg.APISource.Retry.TimeoutMs)
		}
	})

	t.Run("Missing base_url fails validation", func(t *testing.T) {
		rawJSON := []byte(`{
			"pipeline_id": "pipe-rest-02",
			"run_id": "run-02",
			"api_source": {
				"endpoint": "/v1/users",
				"pagination": {"type": "page_number", "page_size": 10},
				"schema": {"fields": [{"name": "id", "type": "int64"}]}
			},
			"destination": {"output_path": "/tmp/out.parquet"}
		}`)
		_, err := domain.ParsePipelineConfig(rawJSON)
		if err == nil {
			t.Fatal("expected error for missing base_url, got nil")
		}
	})
}

func TestSourceConfig_ChunkSizeBytes(t *testing.T) {
	jsonConfig := `{
		"pipeline_id": "test-chunk-cfg",
		"run_id": "run-01",
		"pipeline_type": "file",
		"source": {
			"type": "local_file",
			"path": "/tmp/test.csv",
			"format": "csv",
			"chunk_size_bytes": 16777216,
			"schema": {
				"fields": [{"name": "id", "type": "int64"}]
			}
		},
		"destination": {
			"type": "local_storage",
			"output_path": "/tmp/out",
			"output_format": "parquet"
		}
	}`

	cfg, err := domain.ParsePipelineConfig([]byte(jsonConfig))
	if err != nil {
		t.Fatalf("unexpected error parsing config: %v", err)
	}

	if cfg.Source.ChunkSizeBytes != 16777216 {
		t.Errorf("expected ChunkSizeBytes=16777216, got %d", cfg.Source.ChunkSizeBytes)
	}
}

func TestPipelineConfig_RequiresGCS(t *testing.T) {
	tests := []struct {
		name     string
		cfg      domain.PipelineConfig
		expected bool
	}{
		{
			name: "Local files only",
			cfg: domain.PipelineConfig{
				Source:      domain.SourceConfig{Path: "testdata/input.csv"},
				Destination: domain.DestinationConfig{OutputPath: "testdata/output.parquet"},
			},
			expected: false,
		},
		{
			name: "Source uses gs://",
			cfg: domain.PipelineConfig{
				Source:      domain.SourceConfig{Path: "gs://bucket/input.csv"},
				Destination: domain.DestinationConfig{OutputPath: "testdata/output.parquet"},
			},
			expected: true,
		},
		{
			name: "Destination uses gs://",
			cfg: domain.PipelineConfig{
				Source:      domain.SourceConfig{Path: "testdata/input.csv"},
				Destination: domain.DestinationConfig{OutputPath: "gs://bucket/output.parquet"},
			},
			expected: true,
		},
		{
			name: "DLQ quarantine uses gs://",
			cfg: domain.PipelineConfig{
				Source:      domain.SourceConfig{Path: "testdata/input.csv"},
				Destination: domain.DestinationConfig{OutputPath: "testdata/output.parquet"},
				DLQConfig:   domain.DLQConfig{QuarantinePath: "gs://bucket/dlq.jsonl"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.RequiresGCS(); got != tt.expected {
				t.Errorf("RequiresGCS() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestPipelineConfig_FailFastOnNegativeValues(t *testing.T) {
	t.Run("Negative database batch_size fails validation", func(t *testing.T) {
		manifest := []byte(`{
			"pipeline_id": "pipe-1",
			"run_id": "run-1",
			"database_source": {
				"driver": "postgres",
				"host": "localhost",
				"table": "users",
				"partition_config": {
					"partition_column": "id",
					"batch_size": -50
				},
				"schema": {
					"fields": [{"name": "id", "type": "int64"}]
				}
			},
			"destination": {
				"type": "local_storage",
				"output_format": "parquet",
				"output_path": "out.parquet"
			}
		}`)
		_, err := domain.ParsePipelineConfig(manifest)
		if err == nil {
			t.Fatal("expected error for negative batch_size, got nil")
		}
	})

	t.Run("Negative source chunk_size_bytes fails validation", func(t *testing.T) {
		manifest := []byte(`{
			"pipeline_id": "pipe-1",
			"run_id": "run-1",
			"source": {
				"type": "file",
				"path": "in.csv",
				"format": "csv",
				"chunk_size_bytes": -1024,
				"schema": {
					"fields": [{"name": "id", "type": "int64"}]
				}
			},
			"destination": {
				"type": "local_storage",
				"output_format": "parquet",
				"output_path": "out.parquet"
			}
		}`)
		_, err := domain.ParsePipelineConfig(manifest)
		if err == nil {
			t.Fatal("expected error for negative chunk_size_bytes, got nil")
		}
	})
}

func TestPipelineConfig_GetSchema_And_RequiresGCS(t *testing.T) {
	schema := domain.Schema{Fields: []domain.Field{{Name: "col1", Type: "string"}}}

	t.Run("GetSchema from Source", func(t *testing.T) {
		cfg := &domain.PipelineConfig{Source: domain.SourceConfig{Schema: schema}}
		if len(cfg.GetSchema().Fields) != 1 {
			t.Error("expected schema from Source")
		}
	})

	t.Run("GetSchema from DatabaseSource", func(t *testing.T) {
		cfg := &domain.PipelineConfig{DatabaseSource: &domain.DatabaseSourceConfig{Schema: schema}}
		if len(cfg.GetSchema().Fields) != 1 {
			t.Error("expected schema from DatabaseSource")
		}
	})

	t.Run("GetSchema from APISource", func(t *testing.T) {
		cfg := &domain.PipelineConfig{APISource: &domain.APISourceConfig{Schema: schema}}
		if len(cfg.GetSchema().Fields) != 1 {
			t.Error("expected schema from APISource")
		}
	})

	t.Run("RequiresGCS detection", func(t *testing.T) {
		cfgLocal := &domain.PipelineConfig{
			Source:      domain.SourceConfig{Path: "/tmp/in.csv"},
			Destination: domain.DestinationConfig{OutputPath: "/tmp/out"},
		}
		if cfgLocal.RequiresGCS() {
			t.Error("expected false for local paths")
		}

		cfgGCSDest := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{OutputPath: "gs://bucket/out"},
		}
		if !cfgGCSDest.RequiresGCS() {
			t.Error("expected true for gs:// destination")
		}

		cfgGCSSrc := &domain.PipelineConfig{
			Source: domain.SourceConfig{Path: "gs://bucket/in.csv"},
		}
		if !cfgGCSSrc.RequiresGCS() {
			t.Error("expected true for gs:// source")
		}

		cfgGCSDLQ := &domain.PipelineConfig{
			DLQConfig: domain.DLQConfig{QuarantinePath: "gs://bucket/dlq"},
		}
		if !cfgGCSDLQ.RequiresGCS() {
			t.Error("expected true for gs:// dlq")
		}
	})
}

func TestPipelineConfig_ValidationErrors(t *testing.T) {
	validSchema := domain.Schema{Fields: []domain.Field{{Name: "id", Type: "int64"}}}

	t.Run("Database validation errors", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			PipelineID: "p1",
			RunID:      "r1",
			DatabaseSource: &domain.DatabaseSourceConfig{
				Driver: "",
			},
			Destination: domain.DestinationConfig{OutputPath: "/tmp/out"},
		}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty driver")
		}

		cfg.DatabaseSource.Driver = "postgres"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for missing host and connection_uri")
		}

		cfg.DatabaseSource.Host = "localhost"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for missing table and query")
		}

		cfg.DatabaseSource.Table = "users"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty schema")
		}

		cfg.DatabaseSource.Schema = validSchema
		cfg.DatabaseSource.PoolConfig.MaxOpenConns = -5
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative max_open_conns")
		}

		cfg.DatabaseSource.PoolConfig.MaxOpenConns = 10
		cfg.DatabaseSource.PoolConfig.MaxIdleConns = -2
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative max_idle_conns")
		}
	})

	t.Run("API validation errors", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			PipelineID: "p1",
			RunID:      "r1",
			APISource: &domain.APISourceConfig{
				BaseURL: "",
			},
			Destination: domain.DestinationConfig{OutputPath: "/tmp/out"},
		}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty base_url")
		}

		cfg.APISource.BaseURL = "http://api.com"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty endpoint")
		}

		cfg.APISource.Endpoint = "/items"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for empty schema")
		}

		cfg.APISource.Schema = validSchema
		cfg.APISource.Pagination.PageSize = -10
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative page_size")
		}

		cfg.APISource.Pagination.PageSize = 100
		cfg.APISource.Pagination.MaxPagesLimit = -1
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative max_pages_limit")
		}

		cfg.APISource.Pagination.MaxPagesLimit = 10
		cfg.APISource.Retry.MaxRetries = -1
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative max_retries")
		}
	})

	t.Run("Destination validation errors", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			PipelineID: "p1",
			RunID:      "r1",
			Source: domain.SourceConfig{
				Path:   "/tmp/in.csv",
				Format: "csv",
				Schema: validSchema,
			},
			Destination: domain.DestinationConfig{
				Type: "rest_api",
			},
		}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for rest_api missing base_url")
		}

		cfg.Destination.Endpoint.BaseURL = "http://sink.com"
		cfg.Destination.APIOptions.BatchSize = -5
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for negative batch_size")
		}
	})
}

func TestResilienceConfig_HalfOpenLimit(t *testing.T) {
	var r domain.ResilienceConfig
	r.ApplyDefaults()
	if r.HalfOpenLimit != 1 {
		t.Errorf("expected default HalfOpenLimit=1, got %d", r.HalfOpenLimit)
	}

	r.HalfOpenLimit = -1
	if err := r.Validate(); err == nil {
		t.Error("expected error for negative half_open_limit, got nil")
	}
}

func TestAllTestdataManifestsValid(t *testing.T) {
	files, err := filepath.Glob("../../testdata/configs/*.json")
	if err != nil {
		t.Fatalf("failed to glob configs: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no manifest files found in testdata/configs")
	}

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("failed to read %s: %v", file, err)
			}
			cfg, err := domain.ParsePipelineConfig(data)
			if err != nil {
				t.Fatalf("ParsePipelineConfig failed on %s: %v", filepath.Base(file), err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate failed on %s: %v", filepath.Base(file), err)
			}
		})
	}
}

func TestSecurityConfig_Parsing(t *testing.T) {
	raw := []byte(`{
		"pipeline_id": "p-sec",
		"run_id": "r-sec",
		"source": {
			"path": "test.csv",
			"schema": {
				"fields": [{"name": "id", "type": "int64"}]
			}
		},
		"destination": {
			"output_path": "out/"
		},
		"security": {
			"sensitive_fields": ["ssn", "password", "api_token"]
		}
	}`)
	cfg, err := domain.ParsePipelineConfig(raw)
	if err != nil {
		t.Fatalf("ParsePipelineConfig failed: %v", err)
	}
	if len(cfg.SecurityConfig.SensitiveFields) != 3 {
		t.Fatalf("expected 3 sensitive fields, got %d", len(cfg.SecurityConfig.SensitiveFields))
	}
	if cfg.SecurityConfig.SensitiveFields[0] != "ssn" {
		t.Errorf("expected sensitive field 'ssn', got %s", cfg.SecurityConfig.SensitiveFields[0])
	}
}

func TestBigQuerySinkConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     domain.BigQuerySinkConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: domain.BigQuerySinkConfig{
				ProjectID:        "p",
				DatasetID:        "ds",
				TableID:          "tbl",
				WriteDisposition: "WRITE_APPEND",
				BatchSize:        500,
			},
			wantErr: false,
		},
		{
			name: "missing dataset_id",
			cfg: domain.BigQuerySinkConfig{
				ProjectID: "p",
				TableID:   "t",
			},
			wantErr: true,
		},
		{
			name: "missing table_id",
			cfg: domain.BigQuerySinkConfig{
				ProjectID: "p",
				DatasetID: "ds",
			},
			wantErr: true,
		},
		{
			name: "invalid write_disposition",
			cfg: domain.BigQuerySinkConfig{
				ProjectID:        "p",
				DatasetID:        "ds",
				TableID:          "t",
				WriteDisposition: "INVALID",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("BigQuerySinkConfig.Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestBigQueryManifest_DoesNotFailDestinationValidation(t *testing.T) {
	cfg := domain.PipelineConfig{
		PipelineID: "test",
		RunID:      "r1",
		Source: domain.SourceConfig{
			Path:   "gs://b/f.csv",
			Format: "csv",
			Schema: domain.Schema{Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}}},
		},
		Destination: domain.DestinationConfig{
			Type: "bigquery",
			BigQueryOptions: domain.BigQuerySinkConfig{
				ProjectID: "p",
				DatasetID: "ds",
				TableID:   "tbl",
			},
		},
		DLQConfig: domain.DLQConfig{QuarantinePath: "gs://b/dlq/"},
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected no validation error for bigquery destination, got: %v", err)
	}
}

func TestSourceConfig_PathsAndAllPaths(t *testing.T) {
	t.Run("AllPaths returns Paths slice when populated", func(t *testing.T) {
		s := domain.SourceConfig{
			Paths: []string{"gs://bucket/file1.csv", "gs://bucket/file2.csv"},
		}
		got := s.AllPaths()
		if len(got) != 2 || got[0] != "gs://bucket/file1.csv" || got[1] != "gs://bucket/file2.csv" {
			t.Fatalf("expected 2 paths, got %+v", got)
		}
	})

	t.Run("AllPaths falls back to single Path", func(t *testing.T) {
		s := domain.SourceConfig{
			Path: "gs://bucket/single.csv",
		}
		got := s.AllPaths()
		if len(got) != 1 || got[0] != "gs://bucket/single.csv" {
			t.Fatalf("expected 1 path, got %+v", got)
		}
	})

	t.Run("Validates successfully when only Paths is provided", func(t *testing.T) {
		cfg := domain.PipelineConfig{
			PipelineID:   "test-pipe",
			RunID:        "run-1",
			PipelineType: "ingestion",
			Source: domain.SourceConfig{
				Paths:  []string{"/tmp/f1.csv", "/tmp/f2.csv"},
				Format: "csv",
				Schema: domain.Schema{Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}}},
			},
			Destination: domain.DestinationConfig{
				Type:       "storage",
				OutputPath: "/tmp/out",
			},
			DLQConfig: domain.DLQConfig{QuarantinePath: "/tmp/dlq"},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid config with paths, got error: %v", err)
		}
		if cfg.RequiresGCS() {
			t.Errorf("expected RequiresGCS to be false for local paths")
		}
	})

	t.Run("RequiresGCS returns true if any path in Paths has gs:// prefix", func(t *testing.T) {
		cfg := domain.PipelineConfig{
			PipelineID:   "test-pipe",
			RunID:        "run-1",
			PipelineType: "ingestion",
			Source: domain.SourceConfig{
				Paths:  []string{"gs://bucket/f1.csv", "gs://bucket/f2.csv"},
				Format: "csv",
				Schema: domain.Schema{Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}}},
			},
			Destination: domain.DestinationConfig{
				Type:       "storage",
				OutputPath: "/tmp/out",
			},
			DLQConfig: domain.DLQConfig{QuarantinePath: "/tmp/dlq"},
		}
		if !cfg.RequiresGCS() {
			t.Errorf("expected RequiresGCS to be true when paths contains gs://")
		}
	})
}
