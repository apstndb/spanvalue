# Agent instructions for `spanvalue`

## Commands

- `make build` - compile all packages.
- `make check` - run formatting, vet, build, tests, and lint in one go.
- `make fmt` - format all Go files.
- `make fmt-check` - fail if tracked Go files are not `gofmt`-clean.
- `go build ./...` - compile the library packages.
- `go test ./...` - run the full test suite.
- `go test ./gcvctor -run '^TestName$'` - run tests matching a name in a specific package. Use `./...` to run matching tests across all packages.
- `go test -v ./...` - matches the GitHub Actions test job.
- `make test-v` - verbose test run, equivalent to the CI test command.
- `golangci-lint run` - run lint locally.
- `make test` and `make lint` - thin wrappers for the Go test and lint commands above.
- `make vet` - run `go vet ./...`.

## Architecture

- The root package formats `cloud.google.com/go/spanner.GenericColumnValue` and `*spanner.Row` values into strings through `FormatConfig`.
- `FormatConfig.FormatColumn` tries `FormatComplexPlugins` first, including for `ARRAY` and `STRUCT`, then falls back to the built-in array/struct handling and scalar formatting when plugins return `ErrFallthrough`.
- `LiteralFormatConfig()`, `SimpleFormatConfig()`, `SpannerCLICompatibleFormatConfig()`, and `JSONFormatConfig()` are constructor functions that return fresh `*FormatConfig` values.
- The exported helpers `FormatRowLiteral`, `FormatColumnLiteral`, `FormatRowSpannerCLICompatible`, `FormatColumnSpannerCLICompatible`, and `FormatRowJSONObject` are thin wrappers around those constructor-backed configs.
- `gcvctor/` builds `spanner.GenericColumnValue` values from Go types, while `internal/` holds escape-sequence, literal, and iterator helpers used by both formatting and construction code.
- JSON output is special-cased by `JSONFormatConfig()` plus `FormatJSONSimpleValue`, which keeps `INT64`, `ENUM`, and raw JSON columns in JSON-compatible forms.

## Conventions

- Target Go 1.23.0 (see `go.mod`), with toolchain `go1.23.2`; `iter.Seq`/range-over-func patterns are used throughout the codebase.
- Keep `sppb` as the alias for `cloud.google.com/go/spanner/apiv1/spannerpb`.
- Preserve copied-test attribution comments in `literal_test.go` and `spanner_cli_compatible_test.go`.
- Use `ErrFallthrough` from `FormatComplexFunc` plugins to defer to the built-in array/struct/scalar logic.
- `IsNull` treats a `spanner.GenericColumnValue` as NULL when its `Value` field is nil or a protobuf `NullValue`; `gcvctor.TypedNull` returns a scalar `NullValue` for all types including `STRUCT` and `ARRAY`. Plugins should check it early when they need custom NULL handling.
- `gcvctor.ArrayValue` and `gcvctor.StructValue` are strict: they do not coerce types, arrays must be homogeneous, and struct field names must line up with values. They return sentinel errors (`ErrTypeMismatch`, `ErrMismatchedCounts`, `ErrNilElementType` for a nil `ArrayValueWithType` element type) on failure.
- Empty variadic `ArrayValue` / `ArrayValueWithType` builds an empty SQL array (length 0), not NULL; use `TypedNull` with `typector.ElemTypeToArrayType` or `typector.ElemCodeToArrayType` for NULL ARRAYs.
- `FormatColumn` and formatting functions return sentinel errors (`ErrUnknownType`, `ErrMismatchedFields`) on failure.
- `gcvctor.Float32Value` and `Float64Value` encode `NaN` and `±Inf` as string values to match Spanner's wire format.
- Tests commonly use `t.Parallel()`, `cmp.Diff`, and `protocmp.Transform()` when comparing protobuf-backed values.
- In `gcvctor` tests, prefer building expected `spanner.GenericColumnValue` values with `typector` and `structpb` (plus literals) instead of other `gcvctor` helpers when those helpers share code with the function under test, so `want` stays an independent oracle.
- For JSON row output, unnamed fields are handled through `UnnamedFieldNamer`/`IndexedUnnamedFieldNamer`; these must return non-empty unique names, otherwise an error is returned (replacing previous `panic` behavior). `nil` means keep empty JSON keys.
- Prefer single quotes for shell commands. In double quotes, escape backticks (e.g., `` ` ``).
- Use `merge` instead of `rebase & force push` for branch management; pull requests are merged using `squash and merge`.
