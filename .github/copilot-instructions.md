# Copilot instructions for `spanvalue`

## Commands

- `make build` - compile all packages.
- `make check` - run formatting, vet, build, tests, and lint in one go.
- `make fmt` - format all Go files.
- `make fmt-check` - fail if tracked Go files are not `gofmt`-clean.
- `go build ./...` - compile the library packages.
- `go test ./...` - run the full test suite.
- `go test ./... -run '^TestName$'` - run one test by name; add a package path such as `./gcvctor` if you want to narrow it further.
- `go test -v ./...` - matches the GitHub Actions test job.
- `make test-v` - verbose test run, equivalent to the CI test command.
- `golangci-lint run` - run lint locally.
- `make test` and `make lint` - thin wrappers for the Go test and lint commands above.
- `make vet` - run `go vet ./...`.

## Architecture

- The root package formats `cloud.google.com/go/spanner.GenericColumnValue` and `*spanner.Row` values into strings through `FormatConfig`.
- `FormatConfig.FormatColumn` tries `FormatComplexPlugins` first, including for `ARRAY` and `STRUCT`, then falls back to built-in array/struct handling and scalar formatting.
- `LiteralFormatConfig`, `SimpleFormatConfig`, and `SpannerCLICompatibleFormatConfig` are reusable package-level configs; `JSONFormatConfig()` is the constructor that returns a fresh config.
- The exported helpers `FormatRowLiteral`, `FormatColumnLiteral`, `FormatRowSpannerCLICompatible`, `FormatColumnSpannerCLICompatible`, and `FormatRowJSONObject` are thin wrappers around those configs.
- `gcvctor/` builds `spanner.GenericColumnValue` values from Go types, while `internal/` holds escape-sequence, literal, and iterator helpers used by both formatting and construction code.
- JSON output is special-cased by `JSONFormatConfig()` plus `FormatJSONSimpleValue`, which keeps `INT64`, `ENUM`, and raw JSON columns in JSON-compatible forms.

## Conventions

- Target Go 1.23.0, with toolchain `go1.23.2`; `iter.Seq`/range-over-func patterns are used throughout the codebase.
- Keep `sppb` as the alias for `cloud.google.com/go/spanner/apiv1/spannerpb`.
- Preserve copied-test attribution comments in `literal_test.go` and `spanner_cli_compatible_test.go`.
- Use `ErrFallthrough` from `FormatComplexFunc` plugins to defer to the built-in array/struct/scalar logic.
- `IsNull` treats a nil `gcv.Value` as NULL; plugins should check it early when they need custom NULL handling.
- `gcvctor.ArrayValue` and `gcvctor.StructValue` are strict: they do not coerce types, arrays must be homogeneous, and struct field names must line up with values.
- `gcvctor.Float32Value` and `Float64Value` encode `NaN` and `±Inf` as string values to match Spanner's wire format.
- Tests commonly use `t.Parallel()`, `cmp.Diff`, and `protocmp.Transform()` when comparing protobuf-backed values.
- For JSON row output, unnamed fields are handled through `UnnamedFieldNamer`/`IndexedUnnamedFieldNamer`; `nil` means keep empty JSON keys.
