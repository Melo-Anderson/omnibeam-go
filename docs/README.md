# OmniBeam-Go Documentation Index

Welcome to the OmniBeam-Go technical documentation suite. This catalog guides engineers, contributors, and architects through the pipeline's internal architecture, development practices, deployment workflows, and performance characteristics.

---

## Documentation Directory

| Document | Description | Target Audience |
| :--- | :--- | :--- |
| [**Clean Code & Architecture Guidelines**](clean-code.md) | Single-responsibility limits ($\le$ 300 LOC), error handling, dependency injection, and idiomatic Go best practices. | All Contributors & Reviewers |
| [**Apache Beam Best Practices**](apache-beam-practices.md) | Beam lifecycle contracts (`Setup`, `StartBundle`, `ProcessElement`, `FinishBundle`), Splittable DoFns (SDF), zero-copy coders, and stateful processing. | Pipeline Developers & Data Engineers |
| [**Adapters & Storage Sinks**](architecture-adapters-and-sinks.md) | Hexagonal architecture mapping, adapter pattern, JIT dynamic Parquet sink, BigQuery batch load sink, and compression codecs (`snappy`, `zstd`, `gzip`). | Storage & Systems Engineers |
| [**Columnar Format & DAG Stage Fusion**](architecture-columnar-and-fusion-impact.md) | In-depth analysis of columnar data layouts, Parquet memory consumption, and Beam runner DAG stage fusion/unfusing optimizations. | Performance Engineers & Architects |
| [**Features & Roadmap**](features-and-roadmap.md) | Comprehensive inventory of implemented enterprise features and prioritized milestone roadmap (P1 through P5). | Product Managers & Tech Leads |
| [**Observability & Telemetry**](observability.md) | OpenTelemetry distributed tracing, Prometheus pipeline metrics, Beam execution counters, and structured logging standards. | SRE & Operations Engineers |
| [**Environment, Docker & Deployment**](environment-docker-debug-deploy.md) | Local emulator setup (GCS/PubSub), unit/integration test execution, Docker packaging, and Google Cloud Dataflow Flex Template deployment. | DevOps & Platform Engineers |

---

## Architectural Highlights

```
                    ┌────────────────────────┐
                    │      Manifest JSON     │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
                    │ Pipeline Configuration │
                    └───────────┬────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
┌───────▼────────┐      ┌───────▼────────┐      ┌───────▼────────┐
│ Streaming Kafka│      │ Splittable GCS │      │Pub/Sub Backlog │
│  Bounded / SDF │      │   Parquet /    │      │  Autoscaling   │
│   Offset Mgmt  │      │  Dynamic Slices│      │  Micro-batches │
└───────┬────────┘      └───────┬────────┘      └───────┬────────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │ Zero-Alloc DoFn Engine │
                    │   sync.Pool Buffers    │
                    │  JIT Schema Projection │
                    └───────────┬────────────┘
                                │
        ┌───────────────────────┴───────────────────────┐
        │                                               │
┌───────▼────────┐                             ┌────────▼────────┐
│  Parquet Sink  │                             │  BigQuery Sink  │
│ Buffered Flush │                             │ File-load Batch │
└────────────────┘                             └─────────────────┘
```

---

## Engineering Plans & Specifications

Historical architecture decisions, technical RFCs, and implementation plans are archived under:
- [`superpowers/plans/`](superpowers/plans/) — Detailed step-by-step implementation designs for performance milestones, memory pooling, and streaming sinks.
