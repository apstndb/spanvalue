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

## Tuple-style STRUCT with Spanner CLI scalars

[SpannerCLICompatibleFormatConfig](https://pkg.go.dev/github.com/apstndb/spanvalue#SpannerCLICompatibleFormatConfig)
matches official [spanner-cli](https://github.com/cloudspannerecosystem/spanner-cli)
output, including bracket-style STRUCT in arrays (`[[1, east]]`). For tuple
parentheses (`[(1, east)]`) while keeping CLI scalar rules, customize the config
returned by the constructor and set
[FormatTupleStruct](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatTupleStruct):

```go
fc := spanvalue.SpannerCLICompatibleFormatConfig()
fc.FormatStruct.FormatStructParen = spanvalue.FormatTupleStruct
```

See [ExampleSpannerCLICompatibleFormatConfig_tupleStruct](https://pkg.go.dev/github.com/apstndb/spanvalue#example-SpannerCLICompatibleFormatConfig-TupleStruct).
Keep product-specific combinations in your application (not as new spanvalue presets).

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

The `writer` package streams rows at several levels: `WriteRow` for
`*spanner.Row` (Spanner client), `WriteStructValues` for `[]*structpb.Value` with
a registered field-type schema (spannerpb + structpb at the boundary), and
`WriteGCVs` when values are already `GenericColumnValue`. Use `writer.Writer`
when an adapter only needs `WriteRow`. Use `writer.FlushWriter` when an adapter
owns both row streaming and finalization.

**Production streaming (Spanner client):** `iter.Metadata` is set only after the first
`Next()`. Do not pass `iter.Metadata` to `WithMetadata` at writer construction—it is still
`nil` there and registers an empty schema. When every query returns at least one row,
`iter.Do` and `WriteRow` are enough (the first row supplies column names). When the result
may be empty but metadata still lists columns, use the `iter.Next` loop below and call
`PrepareRowType(iter.Metadata.GetRowType())` on the first loop iteration (even when `Next` returns
`iterator.Done`), then `WriteRow` in the loop and `return w.Flush()` after the loop (do not
`defer w.Flush()`—that discards Flush errors). You do not need to build `[]GenericColumnValue` per row or call
`WriteStructValues` on that path. If you already hold `*sppb.ResultSetMetadata` outside a
`RowIterator` (for example an in-memory `spannerpb.ResultSet`), `WithMetadata(md)` at
construction is fine.

**In-memory fixtures:** after `WithMetadata` or `WithRowType`, prefer `WriteStructValues`
with `[]*structpb.Value` cells (or `WriteGCVs` with `gcvctor` helpers). Do not zip
`RowType` field types with `ListValue` cells manually when types are already registered.
`DelimitedWriter` and `JSONLWriter` preserve explicit duplicate column names.
Empty column names are the only names passed to `UnnamedFieldNamer`, and
generated names avoid collisions with existing explicit names. Set
`UnnamedFieldNamer` to `nil` when callers need empty names to remain empty.

Call `Flush` after the final row when using `writer.FlushWriter`; see the
`Writer`, `FlushWriter`, and `Flusher` godoc for the interface lifecycle
contract.

With the [Spanner client](https://pkg.go.dev/cloud.google.com/go/spanner#section-readme),
export through a `RowIterator` with `WriteRow` and `writer.WithHeader(true)` (default).

When every query returns at least one row, `iter.Do` is enough—`WriteRow` picks up
column names from the first row and writes the header before the first data row:

```go
iter := txn.Query(ctx, stmt)

w := writer.NewDelimitedWriter(
	out,
	'\t',
	writer.WithFormatter(cfg),
	writer.WithHeader(true), // false for headerless CSV/TSV
)
if err := iter.Do(func(row *spanner.Row) error {
	return w.WriteRow(row)
}); err != nil {
	return err
}
return w.Flush()
```

When a `SELECT` may return zero rows but metadata still lists result columns, use
`writer.WriteRowIterator` (or the generic `writer.RunRowIterator` with hooks). It
registers `iter.Metadata` on the first `Next`, streams rows, and finishes with
`Flush` (header-only when no data rows were written). It returns `iter.Metadata`
and query stats even when the iterator is empty:

```go
iter := txn.QueryWithStats(ctx, stmt) // WriteRowIterator stops iter for us

w := writer.NewDelimitedWriter(
	out,
	'\t',
	writer.WithFormatter(cfg),
	writer.WithHeader(true),
)
result, err := writer.WriteRowIterator(iter, w)
if err != nil {
	return err
}
_ = result.Metadata
_ = result.Stats.QueryStats
```

An equivalent manual loop — call `PrepareRowType(iter.Metadata.GetRowType())`
after the first `Next`, write each row, then `Flush` — is still supported;
prefer `WriteRowIterator` when you also need metadata or query stats from an
empty result.

### Schema registration

| Goal | API |
|------|-----|
| Streaming `RowIterator` | [`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator) / [`RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator), or `PrepareRowType` after first `Next` then `WriteRow` (or `iter.Do` when every query has ≥1 row) |
| Known column names only | `WithColumnNames` / `PrepareColumnNames` (at least one name) |
| Names and types from metadata | `WithRowType` / `PrepareRowType` / `WithMetadata` (when `md` is already available) |
| Zero columns (e.g. DML without `THEN RETURN`) | `PrepareRowType(nil)` or `PrepareRowType(metadata.GetRowType())` when fields are empty—**not** `PrepareColumnNames` |
| Zero-row `SELECT` (columns in metadata, no rows) | `PrepareRowType` after first `Next`, then `Flush` for header-only CSV |

Registration is not the same as having zero columns: without `Prepare*` / `With*` and
with no row written, `Flush` or `WriteHeader` returns `writer.ErrMissingColumnNames`.
`PrepareColumnNames` with an empty slice returns `writer.ErrMissingColumnNames`;
`WithColumnNames([])` at construction is ignored (writer stays unregistered). See
`go doc writer`, sections "Column names and field types" and
"Registered schema vs missing schema".

### Result-set shape vs CSV output

| Metadata | Rows | `Flush` with `Header=true` |
|----------|------|----------------------------|
| Zero fields (partitioned DML, etc.) | 0 | Empty file (no header line) |
| Normal `SELECT` row type | 0 | Header row only |
| Normal `SELECT` row type | ≥1 | Header then data rows |

DML or other statements with no result columns: call `PrepareRowType` on
`iter.Metadata.GetRowType()` (possibly nil or zero fields), then `Flush`—do not rely on
`PrepareColumnNames` with an empty name list.

### ENUM and PROTO in CSV

Delimited output uses [`SimpleFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#SimpleFormatConfig)
by default. Build cells with [`gcvctor.EnumValue`](https://pkg.go.dev/github.com/apstndb/spanvalue/gcvctor#EnumValue)
and [`gcvctor.ProtoValue`](https://pkg.go.dev/github.com/apstndb/spanvalue/gcvctor#ProtoValue),
then `WriteGCVs` (see `TestDelimitedWriterWriteGCVsEnumProto` in the `writer` package).
Delimited, JSONL, and SQL encodings differ after
spanvalue formats each column; see the `writer` package documentation. For
non-streaming paths, use
`writer.RowData`, `writer.FormatDelimitedRow`, or `writer.FormatJSONLRow`
directly. Pass the JSON field-name policy explicitly, for example:

```go
line, err := writer.FormatJSONLRow(
	spanvalue.JSONFormatConfig(),
	row,
	spanvalue.IndexedUnnamedFieldNamer,
)
```

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

TSV output uses the same CSV-style writer with a tab delimiter. `NewCSVWriter`
is a thin helper for `NewDelimitedWriter(out, writer.Comma)`. Pass
`writer.Comma` when using the generic delimited constructor for CSV output.
Delimiters must be non-zero valid runes other than `"`, `\r`, `\n`, or
`utf8.RuneError`.

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

SQL INSERT output uses Spanner GoogleSQL quoting. Use
`writer.WithSQLInsertKind` for `INSERT OR IGNORE` or `INSERT OR UPDATE`; see
[INSERT DML syntax](https://cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax).

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
