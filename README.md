# spanvalue

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spanvalue.svg)](https://pkg.go.dev/github.com/apstndb/spanvalue)

Helpers for working with Cloud Spanner’s [`spanner.GenericColumnValue`](https://pkg.go.dev/cloud.google.com/go/spanner#GenericColumnValue) and related client types: **format** values to text (literals, JSON, CLI-style output) and **construct** values from Go types.

| Package | Role |
|--------|------|
| [`github.com/apstndb/spanvalue`](https://pkg.go.dev/github.com/apstndb/spanvalue) | Format `spanner.GenericColumnValue` and `*spanner.Row` using [`FormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatConfig) and presets such as [`LiteralFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#LiteralFormatConfig), [`JSONFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#JSONFormatConfig), [`SpannerCLICompatibleFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#SpannerCLICompatibleFormatConfig). |
| [`github.com/apstndb/spanvalue/gcvctor`](https://pkg.go.dev/github.com/apstndb/spanvalue/gcvctor) | Build `spanner.GenericColumnValue` (scalars, `ARRAY`, `STRUCT`, typed nulls). Types are often composed with [`github.com/apstndb/spantype/typector`](https://pkg.go.dev/github.com/apstndb/spantype/typector). |
| [`github.com/apstndb/spanvalue/writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer) | Stream Spanner rows to CSV, JSONL, or SQL INSERT statements using spanvalue formatters. |

## Upgrade notes for `v0.2.x` -> `v0.3.x`

The main upgrade work in the `v0.3.x` line is the removal of deprecated `gcvctor`
aliases. The formatter APIs are largely additive; most downstream breakage comes
from alias cleanup.

| Removed alias | Replacement |
| --- | --- |
| `BytesBasedValue` | `BytesBasedValueOf` |
| `StringBasedValue` | `StringBasedValueFromCode` |
| `ArrayValueWithType` | `ArrayValueOf` |
| `StructValue` | `StructValueOf` |
| `SimpleTypedNull` | `NullFromCode` |
| `TypedNull` | `NullOf` |
| `ArrayTypeTypedNull` | `NullArrayOf` |
| `ArrayCodeTypedNull` | `NullArrayFromCode` |
| `ElemTypeToEmptyArray` | `EmptyArrayOf` |
| `ElemTypeCodeToEmptyArray` | `EmptyArrayFromCode` |

If you build arrays with nullable elements, `v0.3.0-beta.2` also adds
`gcvctor.NormalizeArrayElements` so callers can rewrite SQL NULL elements to
typed `NullOf(elemType)` values before passing them to the strict `ArrayValueOf`
constructor.

## Identifier quoting helpers

`QuoteIdentifier` and `QuoteQualifiedIdentifier` are conservative quoting
helpers. They always quote for the selected dialect, escape embedded quote
characters, and do **not** attempt a minimal "quote only when necessary"
strategy.

- `DATABASE_DIALECT_UNSPECIFIED` follows the Spanner default and uses GoogleSQL
  quoting.
- `QuoteQualifiedIdentifier` quotes each dotted path segment independently.
- The helpers do not validate empty identifiers or empty path segments; callers
  that reject those shapes must do so before calling them.

```go
quotedTable := spanvalue.QuoteQualifiedIdentifier(
    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
    "analytics.daily_metrics",
)
quotedColumn := spanvalue.QuoteIdentifier(
    databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
    "select",
)
// quotedTable == "`analytics`.`daily_metrics`"
// quotedColumn == "`select`"
```

## Adoption snippets

Use the small helper APIs directly when replacing ad hoc downstream formatting
code:

```go
jsonLine, err := spanvalue.FormatRowJSONObjectFromColumns(
    spanvalue.JSONFormatConfig(),
    columnNames,
    gcvs,
    spanvalue.IndexedUnnamedFieldNamer,
)
```

```go
w := writer.NewSQLInsertWriter(out, "analytics.daily_metrics")
err := w.WriteValues(columnNames, gcvs)
```

## Related: PostgreSQL dialect probes

Integration tests that exercise the Spanner **client** with PostgreSQL dialect (`TypeAnnotation` on query params and row metadata) are maintained in [`github.com/apstndb/spanpg`](https://github.com/apstndb/spanpg) (`integration/pgtypeannotation`), not in this repository.
