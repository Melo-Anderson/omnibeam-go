# Real-Time Telemetry & Observability Guide

OmniBeam compute engine provides **Dataflow-style real-time observability**, collecting distributed throughput, I/O rates, per-worker processing, and dead-letter statistics via **OpenTelemetry**, **Prometheus**, **Jaeger**, and **Grafana**.

---

## 1. Observability Stack Architecture

```
┌────────────────────────────────────────────────────────┐
│                 OmniBeam Compute Workers               │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Worker 1 (Instance ID: worker-01)                │  │
│  │   • recordsCounter.Add(ctx, 1, stage, status)    │  │
│  │   • bytesCounter.Add(ctx, n, stage="sink")       │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Worker 2 (Instance ID: worker-02)                │  │
│  │   • recordsCounter.Add(ctx, 1, stage, status)    │  │
│  └──────────────────────────────────────────────────┘  │
└──────────────────────────┬─────────────────────────────┘
                           │ (OTLP gRPC :4317)
                           ▼
┌────────────────────────────────────────────────────────┐
│             OpenTelemetry Collector Contrib            │
│  ┌─────────────────────────┬────────────────────────┐  │
│  │ Traces Pipeline         │ Metrics Pipeline       │  │
│  │ (OTLP Exporter)         │ (Prometheus Exporter)  │  │
│  └────────────┬────────────┴────────────┬───────────┘  │
└───────────────┼─────────────────────────┼──────────────┘
                │ (:4317)                 │ (:8889)
                ▼                         ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│          Jaeger          │   │        Prometheus        │
└───────────────┬──────────┘   └──────────┬───────────────┘
                │                         │ (PromQL queries)
                ▼                         ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│     Jaeger Web UI        │   │         Grafana          │
│    (localhost:16686)     │   │     (localhost:3000)     │
└──────────────────────────┘   └──────────────────────────┘
```

---

## 2. Service Endpoints & Web UIs

When running locally with `docker compose -f docker-compose.test.yml up -d jaeger otel-collector prometheus grafana`:

| Service | Port / URL | Description | Credentials |
|---|---|---|---|
| **Grafana** | [http://localhost:3000](http://localhost:3000) | Dataflow-style throughput & metrics dashboard | Anonymous / Admin (No login required) |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | PromQL query runner and raw metrics explorer | None |
| **Jaeger UI** | [http://localhost:16686](http://localhost:16686) | Distributed tracing, span waterfalls, and latency | None |
| **OTel Collector Metrics** | [http://localhost:8889/metrics](http://localhost:8889/metrics) | Scrape target for Prometheus (`otel-collector:8889`) | None |

---

## 3. Dual Observability Layers

OmniBeam combines two complementary observability paradigms:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                                Dual Observability System                             │
├──────────────────────────────────────────┬───────────────────────────────────────────┤
│ 1. Real-Time Telemetry (OTel & Grafana)  │ 2. Batch Execution Artifact (metrics.json)│
├──────────────────────────────────────────┼───────────────────────────────────────────┤
│ • In-flight record counters              │ • Post-execution JSON summary in output   │
│ • Real-time throughput rates (MB/s)      │ • Streaming MD5 payload checksum          │
│ • Worker-level latency distributions     │ • Per-column null and invalid statistics  │
│ • Distributed spans across Beam bundles  │ • Total byte and file counts              │
└──────────────────────────────────────────┴───────────────────────────────────────────┘
```

### 3.1. Apache Beam SDK Native Counters (`internal/beam/core`)
Declared at package scope to survive worker serialization across distributed bundles:
- `valid_records`: Counter incremented for every record passing schema validation.
- `dlq_records`: Counter incremented for records failing schema/type validation.
- `audit_events`: Counter tracking compliance, warnings, and error audit events.
- `validation_latency_ms`: Distribution measuring record casting and validation latency.

### 3.2. Post-Execution Summary Writer (`cmd/pipeline/metrics_writer.go`)
Calculates and persists deterministic execution metadata upon pipeline completion:
```json
{
  "pipeline_id": "e2e-sql-01-pg-int",
  "run_id": "run-sql-001",
  "rows_written": 10000,
  "dead_letter_count": 0,
  "bytes_written": 675741,
  "files_written": 1,
  "checksum": "a7b3fe5ab92528085bab2d08b437a736"
}
```

---

## 4. Environment Variables Reference

Telemetry is purely opt-in and non-blocking:

| Environment Variable | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `""` | Target OTLP gRPC endpoint (e.g. `localhost:4317` or `jaeger:4317`). If empty, telemetry is a zero-cost no-op. |
| `WORKER_ID` | Hostname | Identifier for the worker instance (mapped to `service.instance.id` for multi-worker breakdown). |
| `OTEL_SERVICE_NAME` | `omnibeam-pipeline` | Name of the service emitted in resource attributes. |
| `OTEL_SDK_DISABLED` | `false` | When set to `"true"`, disables all OpenTelemetry SDK initialization. |
| `OTEL_METRICS_EXPORTER` | `""` | Set to `"none"` to disable metric exports specifically while keeping tracing enabled. |
| `OTEL_TRACES_EXPORTER` | `""` | Set to `"none"` to disable trace exports specifically while keeping metrics enabled. |

---

## 5. Emitted Metrics Reference

All metrics are emitted using OpenTelemetry standard instruments:

| Metric Name | Type | Unit | Attributes | Description |
|---|---|---|---|---|
| `omnibeam_records_processed_total` | Counter | `{records}` | `pipeline_id`, `run_id`, `stage`, `status` | Cumulative count of records flowing through pipeline stages (`extractor_valid`, `extractor_dlq`). |
| `omnibeam_bytes_written_total` | Counter | `By` (Bytes) | `pipeline_id`, `run_id`, `stage` | Cumulative byte count committed to destination sinks (Parquet, CSV, etc.). |
| `omnibeam_records_per_second` | Gauge | `{records}/s` | `pipeline_id`, `run_id`, `stage`, `status` | Calculated processing rate per stage (available for both batch and streaming jobs). |
| `omnibeam_throughput_mb_per_second` | Gauge | `MB/s` | `pipeline_id`, `run_id`, `stage` | Calculated I/O write rate to destination sinks in MB/s. |

---

## 6. PromQL Query Cookbook

Use these PromQL queries in Prometheus or custom Grafana dashboards:

### 1. Stage Throughput (Elements / sec — Batch & Streaming)
```promql
sum by (stage) (omnibeam_records_per_second) or sum by (stage) (rate(omnibeam_records_processed_total[30s]))
```

### 2. Multi-Worker Breakdown (Dataflow-style Throughput per Worker)
```promql
sum by (exported_instance) (omnibeam_records_per_second) or sum by (exported_instance) (rate(omnibeam_records_processed_total[30s]))
```

### 3. Destination Write Speed (MB / sec)
```promql
sum(omnibeam_throughput_mb_per_second) or (sum(rate(omnibeam_bytes_written_total[30s])) / (1024 * 1024))
```

### 4. DLQ / Quarantine Routing Rate (Records / sec)
```promql
sum by (status) (rate(omnibeam_records_processed_total{status="dlq"}[30s]))
```

### 5. Total Volume Processed
```promql
sum(omnibeam_records_processed_total)
```

---

## 7. Zero-Overhead Guarantees

1. **Pre-allocated Attributes**: Metric attributes (`metric.WithAttributes`) are hoisted outside the extraction loops to avoid heap allocations per record.
2. **Atomic In-Memory Updates**: OpenTelemetry counters perform lock-free atomic increments (~5-10ns overhead).
3. **Asynchronous Background Export**: OTLP network transmission occurs on a dedicated background goroutine (`sdkmetric.PeriodicReader`) without blocking the data stream.
