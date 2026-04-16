# spanvalue

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spanvalue.svg)](https://pkg.go.dev/github.com/apstndb/spanvalue)

Helpers for working with Cloud Spanner’s [`spanner.GenericColumnValue`](https://pkg.go.dev/cloud.google.com/go/spanner#GenericColumnValue) and related client types: **format** values to text (literals, JSON, CLI-style output) and **construct** values from Go types.

| Package | Role |
|--------|------|
| [`github.com/apstndb/spanvalue`](https://pkg.go.dev/github.com/apstndb/spanvalue) | Format `spanner.GenericColumnValue` and `*spanner.Row` using [`FormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatConfig) and presets such as [`LiteralFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#LiteralFormatConfig), [`PostgreSQLLiteralFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#PostgreSQLLiteralFormatConfig), [`JSONFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#JSONFormatConfig), [`SpannerCLICompatibleFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#SpannerCLICompatibleFormatConfig). |
| [`github.com/apstndb/spanvalue/gcvctor`](https://pkg.go.dev/github.com/apstndb/spanvalue/gcvctor) | Build `spanner.GenericColumnValue` (scalars, `ARRAY`, `STRUCT`, typed nulls). Types are often composed with [`github.com/apstndb/spantype/typector`](https://pkg.go.dev/github.com/apstndb/spantype/typector). |
| [`github.com/apstndb/spanvalue/writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer) | Stream Spanner rows to CSV, JSONL, or SQL INSERT statements using spanvalue formatters. |

## Related: PostgreSQL dialect probes

Integration tests that exercise the Spanner **client** with PostgreSQL dialect (`TypeAnnotation` on query params and row metadata) are maintained in [`github.com/apstndb/spanpg`](https://github.com/apstndb/spanpg) (`integration/pgtypeannotation`), not in this repository.
