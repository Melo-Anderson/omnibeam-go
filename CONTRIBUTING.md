# Contributing to OmniBeam-Go

Thank you for your interest in contributing to OmniBeam-Go! We welcome contributions to our high-throughput Apache Beam data pipeline engine.

To ensure consistency, performance, and code quality across the codebase, please review the guidelines below before submitting a pull request.

---

## Code of Conduct

All contributors and maintainers are expected to adhere to our [Code of Conduct](CODE_OF_CONDUCT.md). Please report any unacceptable behavior to project maintainers.

---

## Language Standard

OmniBeam-Go follows a strict English-only policy for technical assets:
- All source code identifiers, comments, logs, and error messages must be in English.
- All documentation, commit messages, and PR discussions must be written in English.

---

## Architectural & Coding Principles

Before submitting code, ensure that your design aligns with our architectural standards:

1. **Single Responsibility & Size Limits**:
   - Each file should focus on a single responsibility.
   - Keep files concise (ideally under 300 lines of executable logic). Break down large transforms into reusable helper functions or sub-packages.
2. **Performance & Zero Allocations**:
   - Hot-path DoFns, coders, and serializers should minimize heap allocations.
   - Use `sync.Pool` for byte buffers and reusable encoders where applicable.
   - Profile changes using benchmarks:
     ```bash
     go test -benchmem -run=^$ -bench=Benchmark ./...
     ```
3. **Apache Beam Best Practices**:
   - DoFns must be stateless or properly handle Beam lifecycle (`Setup`, `StartBundle`, `ProcessElement`, `FinishBundle`, `Teardown`).
   - DoFns must never leak global state across bundles or worker threads.
   - Use Splittable DoFns (SDF) for I/O reading to allow runner-side autoscaling and dynamic work rebalancing.

---

## Development Workflow

### Prerequisites
- **Go**: Version 1.26 or higher.
- **Docker**: For local emulator testing (GCS emulator, Pub/Sub emulator).

### Setting Up the Repository
Clone the repository:
```bash
git clone https://github.com/your-org/omnibeam-go.git
cd omnibeam-go
go mod download
```

### Running Tests
All packages must pass local testing before creating a PR:
```bash
# Run unit tests
go test -short -p 1 ./...

# Run race detection on core packages
go test -race -short ./internal/beam/streams/...

# Run linting
golangci-lint run ./...
```

---

## Commit Guidelines

We use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` A new feature or pipeline transform
- `fix:` A bug fix or pipeline stability correction
- `perf:` A code change that improves throughput or reduces heap allocations
- `docs:` Documentation only changes
- `refactor:` Code refactoring without changing pipeline semantics
- `test:` Adding or updating tests
- `ci:` Changes to CI configuration or build scripts

**Example**:
```text
feat(parquet): add dynamic schema projection for beam parquet sink
perf(codecs): use buffer pool in snappy compressor hot path
```

---

## Pull Request Process

1. Create a descriptive feature branch from `main`:
   ```bash
   git checkout -b feat/my-new-transform
   ```
2. Write unit tests covering both positive and boundary cases.
3. Ensure `go test -short -p 1 ./...` and `golangci-lint run ./...` pass with zero errors.
4. Fill out the [Pull Request Template](.github/pull_request_template.md).
5. Address any feedback from automated CI or maintainer code review.
