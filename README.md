# spanvalue

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spanvalue.svg)](https://pkg.go.dev/github.com/apstndb/spanvalue)

Helpers for working with Cloud Spanner’s [`spanner.GenericColumnValue`](https://pkg.go.dev/cloud.google.com/go/spanner#GenericColumnValue) and related client types: **format** values to text (literals, JSON, CLI-style output) and **construct** values from Go types.

| Package | Role |
|--------|------|
| [`github.com/apstndb/spanvalue`](https://pkg.go.dev/github.com/apstndb/spanvalue) | Format `spanner.GenericColumnValue` and `*spanner.Row` using [`FormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatConfig) and presets such as [`LiteralFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#LiteralFormatConfig), [`JSONFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#JSONFormatConfig), [`SpannerCLICompatibleFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#SpannerCLICompatibleFormatConfig). |
| [`github.com/apstndb/spanvalue/gcvctor`](https://pkg.go.dev/github.com/apstndb/spanvalue/gcvctor) | Build `spanner.GenericColumnValue` (scalars, `ARRAY`, `STRUCT`, typed nulls). Types are often composed with [`github.com/apstndb/spantype/typector`](https://pkg.go.dev/github.com/apstndb/spantype/typector). |
| [`github.com/apstndb/spanvalue/writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer) | Stream Spanner rows to CSV, TSV, JSONL, or SQL INSERT statements using spanvalue formatters. |

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
if err := w.WriteValues(columnNames, gcvs); err != nil {
    return err
}
return w.Flush()
```

## Streaming row exports

The `writer` package accepts `*spanner.Row` values directly through `WriteRow`.
Use `writer.Writer` when an adapter only needs row streaming. Use
`writer.Flusher` alongside `writer.Writer` when an adapter owns the full write
lifecycle.
`CSVWriter` and `JSONLWriter` preserve explicit duplicate column names. Empty
column names are the only names passed to `UnnamedFieldNamer`, and generated
names avoid collisions with existing explicit names. Set `UnnamedFieldNamer` to
`nil` when callers need empty names to remain empty.

Call `Flush` after the final row when the writer also implements
`writer.Flusher`; see the `Writer` and `Flusher` godoc for the interface
lifecycle contract.

CSV output:

```go
func writeCSV(out io.Writer, rows []*spanner.Row) error {
	w := writer.NewCSVWriter(out)
	for _, row := range rows {
		if err := w.WriteRow(row); err != nil {
			return err
		}
	}
	return w.Flush()
}
```

TSV output uses the same CSV-style writer with a tab delimiter. Set the
delimiter before the first write; it follows the same validity rules as
`encoding/csv.Writer.Comma`.

```go
func writeTSV(out io.Writer, rows []*spanner.Row) error {
	w := writer.NewDelimitedWriter(out, '\t')
	for _, row := range rows {
		if err := w.WriteRow(row); err != nil {
			return err
		}
	}
	return w.Flush()
}
```

JSONL output:

```go
func writeJSONL(out io.Writer, rows []*spanner.Row) error {
	w := writer.NewJSONLWriter(out)
	for _, row := range rows {
		if err := w.WriteRow(row); err != nil {
			return err
		}
	}
	return w.Flush()
}
```

SQL INSERT output:

```go
func writeInserts(out io.Writer, table string, rows []*spanner.Row) error {
	w := writer.NewSQLInsertWriter(out, table)
	for _, row := range rows {
		if err := w.WriteRow(row); err != nil {
			return err
		}
	}
	return w.Flush()
}
```

## Related: PostgreSQL dialect probes

Integration tests that exercise the Spanner **client** with PostgreSQL dialect (`TypeAnnotation` on query params and row metadata) are maintained in [`github.com/apstndb/spanpg`](https://github.com/apstndb/spanpg) (`integration/pgtypeannotation`), not in this repository.
