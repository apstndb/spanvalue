# Repository Guidelines

## Project Structure & Module Organization

This repository is a small Go module centered on Spanner type formatting.

- Root package `spantype`: core formatting logic in `format.go` with tests in `format_test.go`.
- `typector/`: helper constructors for `*spannerpb.Type` and struct fields, used heavily by tests and downstream callers.
- `cmd/spantype/`: example CLI that reads protobuf JSON from stdin and prints formatted field types.
- `.github/workflows/go.yml`: CI job that runs the test suite on pushes and pull requests to `main`.

## Build, Test, and Development Commands

Use the standard Go toolchain declared in [`go.mod`](./go.mod).

- `go test ./...`: run all package tests.
- `go test -run TestFormatType ./...`: run a focused test while iterating.
- `go build ./...`: verify all packages, including `cmd/spantype`, compile cleanly.
- `go vet ./...`: catch common static-analysis issues before opening a PR.

For the CLI, a quick manual check is:

```sh
echo '{"fields":[]}' | go run ./cmd/spantype
```

## Coding Style & Naming Conventions

Follow standard Go formatting with `gofmt`; use tabs and keep imports `gofmt`-sorted. Exported identifiers use `PascalCase` (`FormatTypeVerbose`), internal helpers use lower camel case (`lastCut`). Keep package names short and lowercase. Prefer explicit, table-driven test cases for format permutations instead of ad hoc assertions.

## Testing Guidelines

Tests live next to the code they cover and use Go’s `testing` package. Name tests `TestXxx` and subtests with descriptive `desc` values, matching the pattern in [`format_test.go`](./format_test.go). There is no published coverage threshold, but new behavior should include regression tests for relevant type codes, mode combinations, and edge cases such as unknown or unnamed fields.

## Commit & Pull Request Guidelines

Recent history uses short, imperative commit subjects with sentence case, for example `Fix minor issues across the codebase (#3)`. Keep commits focused and explain user-visible behavior changes in the PR body. PRs should include:

- a brief summary of the change,
- linked issue or rationale when applicable,
- test evidence such as `go test ./...`,
- example input/output when CLI formatting changes.

## Release Notes

Use GitHub Releases for release notes instead of maintaining a repository changelog. Start from GitHub's auto-generated notes, then review `git log` for the range being released and add any missed consumer-facing changes such as new `typector` helpers, formatting-mode behavior, or compatibility notes.
