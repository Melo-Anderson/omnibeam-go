# Development Best Practices — Data Platform (Go / Apache Beam)

This document unifies the **Clean Code**, **Clean Architecture**, **DDD**, and **TDD** practices
mapped directly to this project. It serves as a normative guide for any developer (human or AI)
evolving the platform.

> **M011 compliance**: this document is written exclusively in English, per the English Language
> Standard mandate.

---

## 1. Clean Architecture — Layered Structure

The platform is structured in three layers with dependencies flowing in a **single direction:
outermost to innermost**. No inner layer may import from an outer layer.

```
┌──────────────────────────────────────────────────────────────────┐
│  Infrastructure (internal/adapters/)                             │
│  ├── Storage   (adapters/storage/  — local filesystem, GCS)      │
│  ├── Sinks     (adapters/sinks/    — Parquet, DLQ, Metadata)     │
│  ├── Sources   (adapters/sources/  — SQL, Mongo, REST APIs)      │
│  └── IO/Build  (adapters/io_pipeline/ — parsers, compression,    │
│                                         charset, builder)        │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Application (internal/beam/)                              │  │
│  │  └── Orchestrator (IngestionOrchestrator)                  │  │
│  │      Extractors   (FileSourceExtractor, SQLSourceExtractor)│  │
│  │      Builders     (BeamSourceBuilder, DAG Builder)         │  │
│  │      SDF Core     (internal/beam/sdf/)                     │  │
│  │      Transforms   (beam/transforms/ — Beam DoFns)          │  │
│  │                                                            │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │  Domain (internal/domain/)                           │  │  │
│  │  │  ├── Entities   (PipelineConfig, GenericRecord, …)   │  │  │
│  │  │  ├── Value Types (DataType, Schema, Field)           │  │  │
│  │  │  └── Metrics    (PipelineMetrics)                    │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  │                                                            │  │
│  │  Ports (internal/ports/)                                   │  │
│  │  └── Interfaces only — no implementations                  │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Composition Root (cmd/pipeline/main.go)                         │
│  └── Only file that wires concrete types to abstract ports       │
└──────────────────────────────────────────────────────────────────┘
```

### Dependency Rule

| Package | May import | Must NOT import |
|---|---|---|
| `internal/domain/` | Standard library only | `internal/ports/`, `internal/adapters/`, `internal/beam/` |
| `internal/ports/` | `internal/domain/`, stdlib | `internal/adapters/`, `internal/beam/` |
| `internal/beam/` | `internal/ports/`, `internal/domain/`, stdlib | `internal/adapters/` (zero adapter imports) |
| `internal/adapters/` | `internal/ports/`, `internal/domain/`, stdlib, third-party | `internal/beam/` |
| `cmd/pipeline/` | All of the above | — (Composition Root) |

> **Classic violation to avoid**: importing `internal/adapters/storage` directly inside
> `internal/beam/`. All I/O must be received as a `ports.*` interface via constructor injection.

---

## 2. Domain-Driven Design (DDD) in Go

### Entities vs. Value Types

In Go there is no class hierarchy. Apply the DDD distinction using idiomatic Go structs:

| Concept | Characteristics | Examples in this project |
|---|---|---|
| **Entity** | Has identity, mutable state | `PipelineConfig`, `GenericRecord`, `PipelineMetrics` |
| **Value type** | No identity, described only by its content | `Schema`, `Field`, `DataType`, `FieldValue` |
| **Dead Letter** | Carries failure context, immutable after construction | `DeadLetterRecord` |

**Rule**: Value types that must never change after construction should not expose pointer receivers
that mutate their fields. Use constructor functions (`NewGenericRecord`, `NewPipelineMetrics`) to
enforce initialization invariants.

### Aggregate Root

`PipelineConfig` is the **aggregate root** for pipeline configuration. It encapsulates:
- `SourceConfig` (type, path, format, charset, compression, schema)
- `DestinationConfig` (type, output format, path, compression)
- `DLQConfig` (enabled, quarantine path, error threshold)

**Rule**: Always access sub-configs through `PipelineConfig`. `ParsePipelineConfig` is the single
entry point and validates the aggregate atomically before returning it.

### Ports as Boundaries (Hexagonal Architecture)

All cross-layer boundaries are defined as Go `interface` in `internal/ports/`:

```go
// internal/ports/storage.go
type StorageReader interface {
    Open(ctx context.Context, uri string) (io.ReadCloser, error)
    List(ctx context.Context, uriPattern string) ([]string, error)
}

type StorageWriter interface {
    CreateTemp(ctx context.Context, finalURI string) (tempURI string, w io.WriteCloser, err error)
    CommitTemp(ctx context.Context, tempURI, finalURI string) error
    AbortTemp(ctx context.Context, tempURI string) error
}
```

This guarantees that the Application layer (`internal/beam/`) never depends on any concrete storage
library. The implementation (`LocalStorage`) lives entirely in `internal/adapters/storage/`.

---

## 3. Test-Driven Development (TDD)

### Test Pyramid

```
           ▲
          /E2E\         ← tests/e2e/ — end-to-end against a real Docker Compose stack
         /─────\
        / Integ \       ← (future) integration tests against a real filesystem or GCS bucket
       /─────────\
      /   Unit    \     ← *_test.go adjacent to each package — unit tests with in-memory fakes
     /─────────────\
```

### Rules for Unit Tests in Go

- **Table-driven tests**: use `[]struct{ name, input, want }` slices with `t.Run(tc.name, ...)`.
  ```go
  for _, tc := range tests {
      t.Run(tc.name, func(t *testing.T) {
          got, err := fn(tc.input)
          if err != nil { t.Fatalf("unexpected error: %v", err) }
          if got != tc.want { t.Errorf("got %v, want %v", got, tc.want) }
      })
  }
  ```
- **Fakes, not mocks**: use small in-memory struct implementations of port interfaces instead of
  reflection-based mock frameworks.
  ```go
  type fakeStorageWriter struct{ written map[string][]byte }
  func (f *fakeStorageWriter) CreateTemp(_ context.Context, uri string) (string, io.WriteCloser, error) { ... }
  func (f *fakeStorageWriter) CommitTemp(_ context.Context, tmp, final string) error { ... }
  func (f *fakeStorageWriter) AbortTemp(_ context.Context, tmp string) error { ... }
  ```
- **F.I.R.S.T**: Fast, Independent, Repeatable, Self-validating, Timely.
  - Each test must be runnable in isolation via `go test ./...`.
  - No test may depend on a prior test's side effects or file system state.
- **Test file naming**: `<file>_test.go` adjacent to the file under test, same package or
  `<pkg>_test` suffix for black-box tests.

### Rules for Apache Beam Transform Tests

- Beam `DoFn` types (`AuditEnricherFn`, `CastAndValidateFn`) are tested by calling
  `ProcessElement` directly — **no beam.Runner** is needed in Phase 1.
- Pass emit functions as closures to capture and validate emitted records.
  ```go
  fn := transforms.NewCastAndValidateFn(schema)
  fn.ProcessElement(rec,
      func(valid *domain.GenericRecord) { /* assert valid record */ },
      func(dlq *domain.DeadLetterRecord) { /* assert DLQ record */ },
  )
  ```

### Engineering Rigor in Tests

#### 1. Property-Based Testing
- Used to validate invariants on complex domain logic (e.g., `Schema.FieldByName` round-trips,
  `PipelineMetrics.IsStrictlyConserved` after any combination of writes and DLQ events).
- Preferred tool: [`rapid`](https://github.com/flyingmutant/rapid) — idiomatic Go property testing.

#### 2. Chaos / Fault Injection
- Simulate I/O failures in adapter tests by wrapping `io.Reader` with a custom `errReader` that
  returns an error at a configured byte offset.
- Validate retry logic in adapters using `avast/retry-go`.

#### 3. Mutation Testing
- Run via Makefile: `make mutation-test`.
- Target packages: `internal/domain/` and `internal/beam/transforms/`.

---

## 4. Clean Code in Go

### Size and Responsibility

- Functions: **5 to 30 lines**. Extract helper functions when a single function exceeds this.
- Files: **maximum 300 lines**. Split by responsibility if exceeded.
- One responsibility per file (**SRP**).

### Naming

Follow [Effective Go](https://go.dev/doc/effective_go) naming conventions:

- Package names: **short, lowercase, singular nouns** (`domain`, `ports`, `storage`, `sinks`).
- Exported types: `UpperCamelCase` — `FileIngestionOrchestrator`, `PipelineConfig`, `ParquetSink`.
- Unexported helpers: `lowerCamelCase` — `buildReaderStream`, `registeredFormats`.
- Avoid stutter: `storage.LocalStorage` not `storage.StorageLocalStorage`.
- Use domain vocabulary: `DeadLetterRecord`, `PipelineMetrics`, `FieldValue` — not `Data`,
  `Info`, `Manager`, `Utils`.

### Types and Interfaces

- Use **typed constants** for domain enumerations:
  ```go
  type DataType string
  const (
      TypeString    DataType = "string"
      TypeInt64     DataType = "int64"
      TypeFloat64   DataType = "float64"
      // ...
  )
  ```
- Prefer `any` over `interface{}` (Go 1.18+). Never use either in domain types without
  a documented justification.
- Keep interfaces **small** (≤ 4 methods, ISP). Define interfaces where they are consumed, not
  where they are implemented. Exception: `internal/ports/` is the explicit boundary layer.

### Error Handling

- Always wrap errors with context: `fmt.Errorf("context: %w", err)`.
- Return `(T, error)` — never panic in library code.
- Error messages must include **the offending value and what was expected**:
  ```go
  // Correct
  return nil, fmt.Errorf("unsupported source format: %q (registered: %v)", cfg.Format, registered)
  // Wrong
  return nil, fmt.Errorf("unsupported format")
  ```
- Domain validation errors are returned from `Validate()` using `errors.New` — no panics.
- Infrastructure errors (I/O, network) are wrapped and propagated; they are never swallowed silently.

### Comments

- Write **WHY**, not **WHAT**.
- Every non-trivial package gets a doc comment: `// Package beam contains …`
- All exported types and functions get a doc comment in standard Go format.
- Preserve intent comments during refactoring — they document design decisions:
  ```go
  // DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
  ```

---

## 5. SOLID Principles in Go

> Go is not an OOP language, but SOLID describes design principles, not syntax. All five apply
> fully using interfaces, structs, and constructor functions.

### S — Single Responsibility Principle (SRP)

Each struct has one reason to change:

```go
// CSVParser only parses CSV — no compression awareness, no charset handling
type CSVParser struct { delimiter rune; hasHeader bool }

// ParquetSink only writes Parquet — no knowledge of DLQ or metadata
type ParquetSink struct { storage ports.StorageWriter; compression string }

// MetadataSink only writes metrics.json and schema.json
type MetadataSink struct { storage ports.StorageWriter }
```

### O — Open/Closed Principle (OCP)

Open for extension, closed for modification. Use **factory registries** instead of `switch`:

```go
// Wrong — adding a new format requires modifying this function
switch cfg.Format {
case "csv":   return parsers.NewCSVParser(...)
case "jsonl": return parsers.NewJSONLParser()
}

// Correct — PipelineBuilder is closed for modification; new formats self-register
var decoderRegistry = map[string]DecoderFactory{
    "csv":   func(cfg *domain.SourceConfig) ports.StreamDecoder { return parsers.NewCSVParser(...) },
    "jsonl": func(cfg *domain.SourceConfig) ports.StreamDecoder { return parsers.NewJSONLParser() },
}
// New formats: RegisterDecoder("parquet", factory) — builder unchanged
```

### L — Liskov Substitution Principle (LSP)

Every concrete adapter must be a valid substitute for its interface. Enforce at **compile time**:

```go
// internal/adapters/storage/local_storage.go
var _ ports.StorageReader = (*LocalStorage)(nil)
var _ ports.StorageWriter = (*LocalStorage)(nil)

// internal/adapters/sinks/parquet_sink.go
var _ ports.ParquetWriter = (*ParquetSink)(nil)
```

If the interface changes and the implementation is not updated, the build fails immediately — not
at runtime.

### I — Interface Segregation Principle (ISP)

Clients must not depend on methods they do not use. Keep interfaces ≤ 4 methods:

```go
// Segregated — the orchestrator injects StorageReader and StorageWriter separately
type StorageReader interface { Open(...); List(...) }
type StorageWriter interface { CreateTemp(...); CommitTemp(...); AbortTemp(...) }

// LocalStorage implements both, but the orchestrator receives them as distinct fields
```

### D — Dependency Inversion Principle (DIP)

High-level modules (`internal/beam/`) must not import low-level modules (`internal/adapters/`).
Both depend on abstractions (`internal/ports/`):

```go
// internal/beam/orchestrator.go — imports ONLY ports and domain
type IngestionOrchestrator struct {
    extractor    ports.RecordExtractor
    recordWriter ports.RecordWriter
    dlqWriter    ports.DLQWriter
    metaWriter   ports.MetadataWriter
}
```

DIP + Composition Root = full testability without a real filesystem or network databases.

---

## 6. Infrastructure Patterns

### Composition Root

`cmd/pipeline/main.go` is the **only** file allowed to:
- Import concrete adapter packages.
- Instantiate concrete types (`storage.NewLocalStorage()`, `sql.NewPostgresSource(db)`).
- Wire them as interfaces to the orchestrator.

```go
// cmd/pipeline/main.go
localStorage := storage.NewLocalStorage()
extractor    := beam.NewFileSourceExtractor(localStorage, builder, decoder)
parquetSink  := sinks.NewParquetSink(localStorage, cfg.Destination.Compression)
dlqSink      := sinks.NewDLQSink(localStorage)
metaSink     := sinks.NewMetadataSink(localStorage)

orchestrator := beam.NewIngestionOrchestrator(extractor, parquetSink, dlqSink, metaSink)
```

### Storage Adapters — Atomic Writes

All writes use **temp → commit** semantics to avoid partial output on failure:

1. `CreateTemp` — write to `.temp-<uuid>-<filename>`
2. `CommitTemp` — `os.Rename(temp, final)` (atomic on POSIX, best-effort on Windows)
3. `AbortTemp` on any error — remove the temp file

`LocalStorage` implements both `StorageReader` and `StorageWriter`. A future GCS adapter will
implement the same ports without any change to the orchestrator.

### Sinks — Write Once, One Format

- `ParquetSink`: writes `[]*domain.GenericRecord` as a Parquet file using atomic write.
- `DLQSink`: writes `[]*domain.DeadLetterRecord` as JSONL quarantine.
- `MetadataSink`: writes `metrics.json` and `schema.json` from `PipelineMetrics` and `Schema`.
- Each sink must have a `var _ ports.X = (*Sink)(nil)` compile-time assertion.

### IO Pipeline Builder — Middleware Chain

`PipelineBuilder` assembles the read pipeline as a chain of stream wrappers:

```
io.Reader (raw file bytes)
  → compression.WrapDecompressor   (gzip / zstd / snappy / bzip2 / none)
  → charsets.WrapNormalizer        (ISO-8859-1 / Windows-1252 → UTF-8)
  → ports.StreamDecoder.Decode     (CSV or JSONL → <-chan *domain.GenericRecord)
```

OCP: adding a new compression codec or parser requires only registering a factory.

### Pure Functions for Format Logic

Compression detection, charset normalization, and path manipulation are **pure functions** at
module scope (not methods on adapter structs). This allows unit testing without any I/O setup.

### Retry and Resilience

- Adapters for external I/O use `avast/retry-go` for transient failure retry with backoff.
- The domain layer never sees retry logic — it is entirely within `internal/adapters/`.

---

## 7. Apache Beam SDK — Phase Boundaries

This project is built in phases. Phase 1 uses a **synchronous DirectRunner-like orchestrator**.
Phase 5+ will introduce full `beam.Pipeline` graph execution (Cloud Dataflow).

### Phase 1 — Synchronous Orchestrator

- `FileIngestionOrchestrator.Run` is a **plain Go function** — no `beam.Scope`, no
  `beam.PCollection`.
- Beam DoFns (`AuditEnricherFn`, `CastAndValidateFn`) are invoked directly via `ProcessElement`.
- `beam.RegisterType` is used only in `audit_enricher.go`'s `init()` for future portability.

### Phase 5+ — Cloud Dataflow

- Full `beam.Pipeline`, `beam.Scope`, `beam.PCollection`, `beam.ParDo`, `direct.Execute`, and
  `dataflowrun.Execute` are introduced at that milestone.

> **Rule**: NEVER mix Phase 1 synchronous orchestrator code with `beam.Scope` in the same function.

---

## 8. Naming Conventions Quick Reference

| Element | Convention | Example |
|---|---|---|
| Package | short, lowercase | `domain`, `sinks`, `storage` |
| Exported type | UpperCamelCase | `FileIngestionOrchestrator` |
| Unexported identifier | lowerCamelCase | `decoderRegistry` |
| Interface | noun or adjective | `StorageReader`, `StreamDecoder` |
| Constructor | `New<Type>` | `NewLocalStorage()`, `NewParquetSink()` |
| Error variable | `Err<Noun>` | `ErrUnsupportedFormat` |
| Test function | `Test<Unit>_<scenario>` | `TestPipelineConfig_Validate_MissingID` |
| Beam DoFn | `<Action>Fn` | `AuditEnricherFn`, `CastAndValidateFn` |

---

## 9. Glossary of Architectural Decisions

| Decision | Rationale |
|---|---|
| `internal/ports/` as explicit boundary layer | Enforces DIP; inner layers never import adapters |
| Atomic temp → commit writes | Prevents partial or corrupt output files on failure |
| Factory registry for decoders | OCP: new formats self-register; the builder is never modified |
| `var _ Port = (*Type)(nil)` compile-time assertions | LSP: interface contract mismatch caught at build time |
| Beam DoFns callable without a runner | Phase 1 testability; full graph execution deferred to Phase 5 |
| `PipelineConfig.Validate()` fills defaults | Single entry point enforces invariants before any execution |
| `MetadataSink` instead of inline writes | SRP: orchestrator coordinates flow only, never performs direct I/O |
| `avast/retry-go` in adapters only | Retry policy is an infrastructure concern, not a domain concern |
| `DeadLetterRecord` preserves raw payload | Quarantine is fully auditable — original bytes kept alongside the error |
| Standardized `ConnectionURI` for SQL sources | KISS: eliminates dialect formatting boilerplate, supports any relational DB driver directly |
| Polymorphic `RecordExtractor` & Unified `IngestionOrchestrator` | Strategy Pattern: single orchestrator coordinates lifecycle and metrics for any input source (Files, SQL, Kafka, REST) |
| Universal Beam DAG (`BeamSourceBuilder`) | OCP & DRY: downstream DAG (enrich, validate, sinks) is built once regardless of the distributed source SDF |
| Generalized `RecordWriter` for destination sinks | LSP & OCP: decouples orchestrators from file formats, allowing interchangeable Parquet, CSV, BigQuery, and Iceberg sinks |
| Centralized Parquet Schema & Row Mapping | DRY & KISS: eliminates duplicated schema generation and value conversion between batch and distributed sinks |
| 3 Orthogonal SDF Families (`ByteStream`, `PartitionQuery`, `PagedAPI`) | OCP & Reusability: categorizes all distributed ingestion into 3 discrete partitioning models, enabling new connectors with zero Beam core changes |
| Abstract Partitioning Contracts (`PartitionedReader`, `PagedAPIReader`) | DIP & ISP: decouples datastores and APIs from execution engines, allowing adapters to be reused in both synchronous DirectRunner and distributed Dataflow SDFs |

---

## 10. Self-Review Checklist (Agent and Human)

### Compilation Correctness
- [ ] All imports in every Go file are actually used — no unused imports.
- [ ] `"reflect"` is imported only if `reflect.TypeOf(...)` is explicitly called.
- [ ] `"fmt"` is imported only if `fmt.Errorf`, `fmt.Sprintf`, or `fmt.Printf` is called.
- [ ] Channel error draining uses `for range errChan { ... }`, not single `<-errChan`.
- [ ] `parquet.OpenFile(f, fi.Size())` — never `parquet.OpenFile(os.Open(...))`.
- [ ] `os.MkdirAll` is called before `os.WriteFile` to paths that may not exist.
- [ ] `defer` inside loops has `//nolint:gocritic` comment explaining the intent.

### Clean Architecture
- [ ] No code in `internal/beam/` or `internal/domain/` imports from `internal/adapters/`.
- [ ] The Composition Root (`cmd/pipeline/main.go`) is the ONLY file that imports concrete adapters.
- [ ] Each orchestrator/use-case struct receives ALL dependencies as interface parameters (DIP).
- [ ] Metadata output is written by `MetadataSink`, not inlined in the orchestrator.

### SOLID
- [ ] Every concrete adapter has a `var _ Port = (*Type)(nil)` compile-time assertion.
- [ ] No `switch` on format/type strings in extensible builders — use registry maps instead.
- [ ] Interfaces in `ports/` have ≤ 4 methods (ISP).
- [ ] Each struct/file has exactly one reason to change (SRP).

### Tests
- [ ] Unit tests are table-driven.
- [ ] No real filesystem or network calls in unit tests — use in-memory fakes.
- [ ] Beam DoFns are tested by calling `Setup()`, `ProcessElement()`, and `Teardown()` directly, not via a runner.
- [ ] Test names follow `Test<Unit>_<scenario>` convention.

### Apache Beam Architecture & DoFn Lifecycle
- [ ] Conforms to all rules in [`docs/apache-beam-practices.md`](apache-beam-practices.md).
- [ ] `beam.RegisterInit` is called ONLY inside package-level `init()` functions (never after `beam.Init()`).
- [ ] DoFn struct exports only serializable state; I/O connections are unexported and managed in `Setup(ctx)` / `Teardown()`.
- [ ] DoFn lifecycle methods strictly adhere to reflection validation rules in `beam/core/graph/fn.go`.

### Configuration & Defaults (KISS & Fail-Fast)
- [ ] `ApplyDefaults()` only hydrates omitted / zero-value fields (`field == 0` or `field == ""`).
- [ ] `Validate()` strictly checks business invariants and rejects negative or absurd values (`field < 0`).
- [ ] Execution layers (Adapters, DoFns, Sinks) never perform defensive `if val <= 0` fallback reassignments — they consume validated config fields directly.

### Governance
- [ ] All code and documentation is written in English (M011).
- [ ] No git state-modifying commands are executed autonomously (M010).
- [ ] OpenTelemetry instrumentation is added for any new adapter or use-case boundary (M009).

---

## 11. Configuration & Default Handling Guidelines (KISS & Fail-Fast)

To preserve the **KISS** (Keep It Simple, Stupid) principle and prevent subtle bugs caused by silent fallbacks, all configuration handling across the platform follows a strict two-stage lifecycle:

### 1. Two-Stage Lifecycle: Hydration vs. Validation

```
Manifest (JSON / YAML)
       │
       ▼
1. ApplyDefaults()     ← ONLY populates omitted / zero-value fields (e.g., field == 0, field == "")
       │
       ▼
2. Validate()          ← STRICTLY validates invariants. Rejects negative or absurd numbers (e.g., field < 0)
       │
       ▼
Execution Runtime      ← Adapters, DoFns, and Sinks CONSUME validated values directly. NO defensive IFs!
```

### 2. Mandatory Rules

1. **Explicit Zero-Value Checks in `ApplyDefaults()`**:
   - Always check `field == 0` or `field == ""` when assigning default values.
   - **NEVER** use `if field <= 0 { field = defaultValue }` during hydration, as this silently masks invalid negative numbers provided by the user.

2. **Strict Fail-Fast in `Validate()`**:
   - Reject negative or invalid tuning values (`if field < 0 { return fmt.Errorf("field cannot be negative, got %d", field) }`).
   - If a caller supplies an invalid parameter (e.g., `"batch_size": -50`), the pipeline must fail immediately at startup with a descriptive error.

3. **Zero Defensive Reassignments in Execution Layers**:
   - Internal components (such as `GenericSQLSource`, `MongoSource`, `HTTPClient`, `PartitionQuerySourceSDF`, sinks) assume that the configuration passed to them is already hydrated and validated.
   - Never repeat fallback logic or wrapper methods like `EffectiveBatchSize()` inside execution classes. Read `cfg.PartitionConfig.BatchSize` directly.
