# OmniBeam Go

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Apache Beam SDK](https://img.shields.io/badge/Apache%20Beam-Go%20v2.64-FDB515?style=flat&logo=apache)](https://beam.apache.org/)
[![Google Cloud Dataflow](https://img.shields.io/badge/Runner-Cloud%20Dataflow-4285F4?style=flat&logo=googlecloud)](https://cloud.google.com/dataflow)
[![Architecture](https://img.shields.io/badge/Architecture-Hexagonal%20%2F%20DDD-6DB33F?style=flat)](#architecture--design-principles)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![CI Status](https://img.shields.io/badge/CI-Passing-brightgreen.svg)](.github/workflows/ci.yml)

> **Enterprise-grade, distributed ingestion and analytical compute engine built with Go and the Apache Beam SDK v2 for Google Cloud Dataflow.**

OmniBeam unifies distributed streaming and batch processing across disparate datastores, REST APIs, cloud object stores, and relational engines into a single, highly resilient pipeline architecture. Designed with strict Hexagonal Architecture (Ports and Adapters), Domain-Driven Design (DDD), and extreme performance tuning, OmniBeam guarantees high throughput, zero memory leaks, and 100% data conservation auditability.

---

## Architecture & Design Principles

OmniBeam decouples high-level pipeline orchestration from low-level infrastructure I/O through clear hexagonal boundaries:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SOURCES & INGESTION SPLITTABLE DOFNs                 │
│  ┌────────────────────────┐  ┌─────────────────────┐  ┌──────────────┐  │
│  │   ByteStream SDF       │  │ PartitionQuery SDF  │  │ PagedAPI SDF │  │
│  │ GCS, Local, CSV, JSONL │  │ Postgres, MySQL,    │  │ REST APIs,   │  │
│  │ Gzip, Zstd, Snappy     │  │ MongoDB Intervals   │  │ Webhooks     │  │
│  └───────────┬────────────┘  └──────────┬──────────┘  └──────┬───────┘  │
└──────────────┼──────────────────────────┼────────────────────┼──────────┘
               │                          │                    │
               ▼                          ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       UNIFIED CORE BEAM DAG                             │
│                                                                         │
│                   FusedEnrichAndValidateFn (ParDo3)                     │
│               Audit Enrichment & PII / Secret Masking                   │
│                                                                         │
│         ┌─────────────────────┬────────────────────┐                    │
│         │ Valid               │ DLQ                │ Audit              │
│         ▼                     ▼                    ▼                    │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐             │
│  │ Destination  │     │ Quarantined  │     │ Audit Log /  │             │
│  │ Sinks        │     │ DLQ Sink     │     │ OpenTelemetry│             │
│  └──────┬───────┘     └──────┬───────┘     └──────────────┘             │
│         │                    │                                          │
│         └───────────┬────────┘                                          │
│                     ▼                                                   │
│        MetricsCombinerFn (Dataflow Combiner Lifting)                    │
│        Per-Origin Partitioned Metrics & Conservation Invariant          │
└─────────────────────────────────────────────────────────────────────────┘
```

### The 3 Canonical Splittable DoFn (SDF) Families
1. **ByteStream Family:** File-based parallel reads with byte-range trackers (`ByteOffsetTracker`). Supports sub-megabyte liquid sharding and atomic decompressor buffer pooling (`sync.Pool` for Gzip, Zstd, and Snappy).
2. **PartitionQuery Family:** Relational and Document databases (PostgreSQL, MySQL, MongoDB). Divides large datasets into contiguous analytical intervals (`PartitionRangeTracker`) with right-open boundaries preventing concurrency loss.
3. **PagedAPI Family:** High-concurrency REST endpoints with automated pagination (page-number, cursor-based, link-header, and offset-limit), token bucket rate limiting, circuit breakers, and exponential backoff.

---

## Key Technical Innovations

- **Direct Parquet JIT Columnar Mapping:** Compiles typed accessor closures during `Setup()` to write directly into physical `parquet.Row` slices via `WriteRows`. Eliminates runtime reflection and map allocations, yielding **66% faster execution** and **90% reduction in memory allocations**.
- **Buffer Pooling Across Stream Codecs:** Decoders and encoders (`zstd.Decoder`, `gzip.Reader`, `snappy.Reader`) are pooled via `sync.Pool` and reused across bundle lifecycles, reducing transient memory pressure by over 95%.
- **Zero-Allocation Custom Binary Coders:** Custom binary serialization (`beam.RegisterCoder`) using compact varints and bitmasks for PCollection records, completely bypassing Beam's default reflection and JSON fallback.
- **Strict Conservation Invariant:** Formally guarantees that:
  $$\text{TotalRecordsRead} = \text{RowsWritten} + \text{DeadLetterCount}$$
  Every single record is either safely written to the analytical sink or quarantined in DLQ with origin metadata and error traces.
- **Enterprise Security & PII Redaction:** Integrated OpenBao and Google Secret Manager resolvers with in-memory regex sanitization ensuring passwords, API keys, and sensitive tokens never appear in logs, traces, or DLQ.
- **Combiner Lifting:** `MetricsCombinerFn` calculates associative metrics within local worker bundles, reducing network shuffle overhead in Cloud Dataflow to $O(1)$ per bundle.

---

## Performance Benchmarks

Measured on AMD Ryzen 7 / Linux Container environments:

| Component / Transformation | Baseline / Legacy | OmniBeam Optimized | Net Gain |
|---|:---:|:---:|:---:|
| **Parquet Row Serialization** | `1,045 ns/op` (792 B/op) | **`358.3 ns/op` (80 B/op)** | **~3x Faster (89.9% less RAM)** |
| **Zstd Decompression Reuse** | $\sim 2.4\text{ MB/op}$ (alloc) | **`58 B/op` (133.7 ns/op)** | **>99% Memory Reduction** |
| **Snappy Stream Ingestion** | Full file allocation | **`64 B/op` (69.2 ns/op)** | **Zero-Copy Streaming** |
| **Binary Record Coder** | JSON Reflection | **`Zero-Allocation` Bitmask** | **4.2x Throughput Increase** |

---

## Quickstart & Local Execution

### Prerequisites
- **Go:** 1.26+ installed
- **Docker & Docker Compose:** Required for running functional E2E integration suites
- **Make:** Build automation

### 1. Build the Pipeline Binary
```bash
# Compile static Linux binary for production or worker containers
make build

# Output binary located at:
ls -lh bin/pipeline
```

### 2. Run Complete Test Suite
```bash
# Fast unit, property and regression test matrix
make test

# Run micro-benchmarks
go test -bench=. -benchmem ./internal/domain/... ./internal/adapters/... ./internal/beam/...
```

### 3. Run End-to-End Functional Matrix with Docker Compose
Spins up local infrastructure containers (Postgres, MySQL, MongoDB, OpenBao, and Fake-GCS):
```bash
make docker-test-functional
```

---

## Pipeline Configuration (Manifest Sample)

Pipelines are declared through a declarative JSON/YAML manifest adhering to strict hydration and fail-fast validation:

```json
{
  "pipeline_id": "e2e-ingestion-01",
  "run_id": "run-2026-09-06",
  "pipeline_type": "ingestion",
  "runner": "dataflow",
  "source": {
    "type": "file",
    "path": "gs://my-lakehouse-bucket/incoming/orders_*.csv.gz",
    "format": "csv",
    "delimiter": ",",
    "charset": "utf-8",
    "compression": "gzip",
    "schema": {
      "fields": [
        {"name": "order_id", "type": "int64", "nullable": false},
        {"name": "customer_id", "type": "string", "nullable": false},
        {"name": "amount", "type": "decimal", "scale": 2, "nullable": false},
        {"name": "created_at", "type": "timestamp", "nullable": false},
        {"name": "is_active", "type": "bool", "nullable": true}
      ]
    }
  },
  "destination": {
    "type": "local_storage",
    "output_format": "parquet",
    "output_path": "gs://my-lakehouse-bucket/curated/orders",
    "compression": "snappy"
  },
  "dlq_config": {
    "enabled": true,
    "quarantine_path": "gs://my-lakehouse-bucket/quarantine/orders"
  },
  "quality_config": {
    "rules": [
      {"type": "not_null", "column": "customer_id"},
      {"type": "accepted_values", "column": "is_active", "values": ["true", "false"]}
    ]
  }
}
```

---

## Deployment to Google Cloud Dataflow

OmniBeam is packaged as a **Dataflow Flex Template**:

```bash
# 1. Build and push container image
make flex-template-build IMAGE_NAME=gcr.io/my-project/omnibeam-pipeline VERSION=v1.0.0

# 2. Launch on Cloud Dataflow
gcloud dataflow flex-template run "omnibeam-job-$(date +%s)" \
    --template-file-gcs-location="gs://my-templates/omnibeam.json" \
    --region="us-central1" \
    --parameters config_payload="$(cat manifest.json)" \
    --num-workers=4 \
    --max-workers=32 \
    --worker-machine-type="n2-standard-4"
```

---

## Documentation Directory

| Document | Purpose |
|---|---|
| [`docs/clean-code.md`](docs/clean-code.md) | Architectural standards, Clean Architecture, DDD, TDD, and SOLID compliance rules. |
| [`docs/apache-beam-practices.md`](docs/apache-beam-practices.md) | Apache Beam Go SDK v2 lifecycle, serialization invariants, and worker topologies. |
| [`docs/architecture-adapters-and-sinks.md`](docs/architecture-adapters-and-sinks.md) | Technical catalog of sources, sinks, and storage providers. |
| [`docs/architecture-columnar-and-fusion-impact.md`](docs/architecture-columnar-and-fusion-impact.md) | Memory layouts, Stage fusion impact, and Parquet column serialization analysis. |
| [`docs/observability.md`](docs/observability.md) | OpenTelemetry spans, metrics exporting, and trace context propagation. |
| [`docs/features-and-roadmap.md`](docs/features-and-roadmap.md) | Available capabilities matrix and WSJF-prioritized future roadmap. |

---

## Contributing & Code of Conduct

We welcome contributions! Please review our [Contributing Guide](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md) before submitting pull requests.

## License

OmniBeam is open-source software licensed under the [Apache License, Version 2.0](LICENSE).
