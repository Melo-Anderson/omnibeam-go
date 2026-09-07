// cmd/pipeline/main.go — Composition Root.
// This is the ONLY file in the project that imports concrete adapter packages.
// All other packages depend exclusively on interfaces (ports.*).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	bq_adapter "github.com/omnibeam/dataflow-compute-go/internal/adapters/bigquery"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/storage"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/telemetry"
	bq_beam "github.com/omnibeam/dataflow-compute-go/internal/beam/bigquery"
	beam_engine "github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	beam.RegisterInit(func() {
		bq_beam.SetGlobalBigQueryWriterProvider(func(ctx context.Context, projectID string) (ports.BigQueryWriter, error) {
			return bq_adapter.NewStorageWriteAdapter(ctx)
		})
	})
}

var (
	configFlag      = flag.String("config", "", "Path to pipeline JSON manifest")
	payloadFlag     = flag.String("config_payload", "", "Inline JSON configuration payload")
	payloadPathFlag = flag.String("config_payload_path", "", "Path to JSON configuration file")
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}
	beam.Init()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	shutdownTracer, err := telemetry.InitTracer(context.Background(), "omnibeam-pipeline")
	if err != nil {
		slog.Warn("failed initializing openTelemetry tracer, falling back to no-op", "error", err)
	} else if shutdownTracer != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := shutdownTracer(shutdownCtx); err != nil {
				slog.Debug("failed flushing telemetry tracer", "error", err)
			}
		}()
	}

	shutdownMeter, err := telemetry.InitMeter(context.Background(), "omnibeam-pipeline")
	if err != nil {
		slog.Warn("failed initializing openTelemetry meter, falling back to no-op", "error", err)
	} else if shutdownMeter != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := shutdownMeter(shutdownCtx); err != nil {
				slog.Debug("failed flushing telemetry meter", "error", err)
			}
		}()
	}

	jsonBytes, err := readManifestPayload(*configFlag, *payloadFlag, *payloadPathFlag)
	if err != nil {
		slog.Error("manifest input error", "error", err)
		os.Exit(1)
	}

	cfg, err := domain.ParsePipelineConfig(jsonBytes)
	if err != nil {
		slog.Error("failed parsing configuration", "error", err)
		os.Exit(1)
	}

	if f := flag.Lookup("runner"); f != nil && f.Value.String() != "" {
		cfg.Runner = f.Value.String()
	}

	slog.Info("starting pipeline", "id", cfg.PipelineID, "run_id", cfg.RunID, "type", cfg.PipelineType, "runner", cfg.Runner)

	if err := executePipeline(context.Background(), cfg); err != nil {
		slog.Error("pipeline execution failed", "error", err)
		os.Exit(1)
	}
}

func readManifestPayload(configPath, payload, payloadPath string) ([]byte, error) {
	if configPath != "" {
		return os.ReadFile(configPath)
	}
	if payload != "" {
		return []byte(payload), nil
	}
	if payloadPath != "" {
		return os.ReadFile(payloadPath)
	}
	return nil, fmt.Errorf("missing required configuration: pass --config, --config_payload, or --config_payload_path")
}

func initStorageResolver(ctx context.Context, cfg *domain.PipelineConfig) (ports.StorageBackend, error) {
	local := storage.NewLocalStorage()
	var gcsBackend ports.StorageBackend

	if cfg.RequiresGCS() {
		gcs, err := storage.NewGCSStorage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed initializing required GCS storage: %w", err)
		}
		gcsBackend = gcs
	}

	return storage.NewStorageResolver(local, gcsBackend), nil
}

func executePipeline(ctx context.Context, cfg *domain.PipelineConfig) error {
	storageBackend, err := initStorageResolver(ctx, cfg)
	if err != nil {
		return err
	}

	secretResolver := buildSecretResolver(ctx, cfg.SecretsConfig)
	schema := cfg.GetSchema()

	sink, err := buildDestinationSink(ctx, cfg, storageBackend, secretResolver, schema)
	if err != nil {
		return err
	}

	source, err := buildSourceEngine(ctx, cfg, storageBackend, secretResolver)
	if err != nil {
		return err
	}

	dlqSink := buildDLQSink(cfg, storageBackend)
	auditSink := buildAuditSink(cfg, storageBackend)

	p, _ := beam.NewPipelineWithRoot()
	metricsCol := beam_engine.BuildPipeline(p, source, sink, dlqSink, auditSink, schema, cfg.QualityConfig, cfg.SecurityConfig)
	beam.ParDo0(p.Root(), beam_engine.NewMetricsSinkDoFn(storageBackend, cfg.GetMetricsOutputDir()), metricsCol)

	if cfg.Runner != "" {
		if f := flag.Lookup("runner"); f != nil {
			_ = f.Value.Set(cfg.Runner)
		}
	}

	if err := beamx.Run(ctx, p); err != nil {
		return fmt.Errorf("beam pipeline execution failed: %w", err)
	}

	writeExecutionMetrics(cfg)
	return nil
}

func buildSecretResolver(ctx context.Context, cfg *domain.SecretsConfig) ports.SecretResolver {
	openbao := secrets.NewOpenBaoSecretResolver(cfg)
	var gcp ports.SecretResolver
	if cfg != nil && cfg.GCPProjectID != "" {
		gcpCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		gcpResolver, err := secrets.NewGCPSecretManagerResolver(gcpCtx, cfg)
		if err != nil {
			slog.Warn("gcp secret manager unavailable, gcp: prefix will fail at resolution time", "error", err)
		} else {
			gcp = gcpResolver
		}
	}
	var defaultProvider string
	if cfg != nil {
		defaultProvider = cfg.Provider
	}
	return secrets.NewCompositeSecretResolver(openbao, gcp, defaultProvider)
}
