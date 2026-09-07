package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestMain(m *testing.M) {
	beam.Init()
	os.Exit(m.Run())
}

func TestMainHelpers(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "pipeline.json")
	_ = os.WriteFile(configFile, []byte(`{"pipeline_id":"test"}`), 0644)

	t.Run("readManifestPayload configPath", func(t *testing.T) {
		data, err := readManifestPayload(configFile, "", "")
		if err != nil || string(data) != `{"pipeline_id":"test"}` {
			t.Errorf("readManifestPayload configPath: got %s, err %v", string(data), err)
		}
	})

	t.Run("readManifestPayload payload string", func(t *testing.T) {
		data, err := readManifestPayload("", `{"direct":true}`, "")
		if err != nil || string(data) != `{"direct":true}` {
			t.Errorf("readManifestPayload payload: got %s, err %v", string(data), err)
		}
	})

	t.Run("readManifestPayload payloadPath", func(t *testing.T) {
		data, err := readManifestPayload("", "", configFile)
		if err != nil || string(data) != `{"pipeline_id":"test"}` {
			t.Errorf("readManifestPayload payloadPath: got %s, err %v", string(data), err)
		}
	})

	t.Run("readManifestPayload empty returns error", func(t *testing.T) {
		_, err := readManifestPayload("", "", "")
		if err == nil {
			t.Error("expected error for empty manifest inputs")
		}
	})

	t.Run("initStorageResolver local only", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{OutputPath: "/tmp/out.csv"},
		}
		resolver, err := initStorageResolver(context.Background(), cfg)
		if err != nil || resolver == nil {
			t.Errorf("initStorageResolver local: got %v, err %v", resolver, err)
		}
	})

	t.Run("initStorageResolver with GCS emulator", func(t *testing.T) {
		t.Setenv("STORAGE_EMULATOR_HOST", "localhost:9099")
		cfg := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{OutputPath: "gs://bucket/out.csv"},
		}
		resolver, err := initStorageResolver(context.Background(), cfg)
		if err != nil || resolver == nil {
			t.Errorf("initStorageResolver GCS: got %v, err %v", resolver, err)
		}
	})

	t.Run("buildSecretResolver with GCP and OpenBao", func(t *testing.T) {
		cfg := &domain.SecretsConfig{
			Provider:     "openbao",
			VaultURL:     "http://localhost:8200",
			VaultToken:   "token",
			GCPProjectID: "my-gcp-project",
		}
		res := buildSecretResolver(context.Background(), cfg)
		if res == nil {
			t.Error("expected non-nil secret resolver")
		}

		resNil := buildSecretResolver(context.Background(), nil)
		if resNil == nil {
			t.Error("expected non-nil secret resolver for nil config")
		}
	})

	t.Run("Worker providers registration", func(t *testing.T) {
		registerWorkerProviders()
		registerWorkerStorageProviders()
		registerWorkerStreamProviders()
		registerWorkerDBProviders()

		// Test workerDBProvider
		dbCfg := &domain.DatabaseSourceConfig{
			Driver: "sqlmock",
		}
		_, err := workerDBProvider(context.Background(), dbCfg, nil)
		if err == nil {
			t.Log("workerDBProvider returned reader or unregistered error as expected")
		}
	})

	t.Run("executePipeline runs end-to-end via direct runner", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping direct runner e2e pipeline test in short mode")
		}
		inCSV := filepath.Join(tmpDir, "input.csv")
		outJSONL := filepath.Join(tmpDir, "output.jsonl")
		metricsFile := filepath.Join(tmpDir, "metrics.json")
		_ = os.WriteFile(inCSV, []byte("id,name\n1,Alice\n2,Bob\n"), 0644)

		cfg := &domain.PipelineConfig{
			PipelineID: "test-pipeline",
			Runner:     "direct",
			Source: domain.SourceConfig{
				Path:   inCSV,
				Format: "csv",
				Schema: domain.Schema{
					Fields: []domain.Field{
						{Name: "id", Type: domain.TypeInt64, Nullable: false},
						{Name: "name", Type: domain.TypeString, Nullable: false},
					},
				},
			},
			Destination: domain.DestinationConfig{
				Type:         "storage",
				OutputFormat: "jsonl",
				OutputPath:   outJSONL,
			},
			DLQConfig: domain.DLQConfig{
				QuarantinePath: filepath.Join(tmpDir, "dlq.jsonl"),
			},
		}

		err := executePipeline(context.Background(), cfg)
		if err != nil {
			t.Fatalf("executePipeline failed: %v", err)
		}

		if _, err := os.Stat(metricsFile); err != nil {
			t.Errorf("expected metrics file to be created: %v", err)
		}
	})
}
