## Description
<!-- Briefly describe the changes introduced by this pull request, the problem it solves, or the feature it implements. -->

## Type of Change
- [ ] 🐛 Bug fix (non-breaking change fixing an issue)
- [ ] ✨ New feature (non-breaking change adding functionality)
- [ ] ⚡ Performance optimization (latency reduction, memory/allocation reduction)
- [ ] ♻️ Refactor (code quality, cleanup, architectural alignment)
- [ ] 📝 Documentation update
- [ ] 🔧 CI/Build/Tooling update

## Architectural & Quality Checklist
- [ ] Code strictly complies with project English-only standards (identifiers, comments, logs).
- [ ] File size limit adhered to (files should focus on a single responsibility, ideally $\le$ 300 lines of executable code).
- [ ] DoFns adhere to Apache Beam lifecycle (`Setup`, `StartBundle`, `ProcessElement`, `FinishBundle`, `Teardown`) and maintain no unauthorized mutable state.
- [ ] Hot-path operations minimize allocations and reuse buffers via `sync.Pool` where appropriate.
- [ ] Unit tests added or updated for all new logic.
- [ ] All tests pass locally via `go test -short -p 1 ./...`.
- [ ] Linter passes with no warnings via `golangci-lint run ./...`.

## Verification & Testing
<!-- Detail how the changes were verified (commands run, benchmark results, emulator tests). -->

```bash
go test -v -short ./...
```

### Benchmark Results (if applicable)
<!-- Paste any `go test -benchmem` comparison before vs after -->
```text
```

## Related Issues
<!-- Link related issues or discussions (e.g. Closes #123) -->
