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
parentheses (`[(1, east)]`) while keeping CLI scalar rules, clone the preset and
set [FormatTupleStruct](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatTupleStruct):

```go
fc := spanvalue.SpannerCLICompatibleFormatConfig().Clone()
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
w, err := writer.NewSQLInsertWriter(out, "analytics.daily_metrics")
if err != nil {
	return err
}
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
`WriteStructValues` on that path. If you already hold `*spannerpb.ResultSetMetadata` outside a
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

w, err := writer.NewDelimitedWriter(
	out,
	'\t',
	writer.WithFormatter(cfg),
	writer.WithHeader(true), // false for headerless CSV/TSV
)
if err != nil {
	return err
}
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

w, err := writer.NewDelimitedWriter(
	out,
	'\t',
	writer.WithFormatter(cfg),
	writer.WithHeader(true),
)
if err != nil {
	return err
}
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

### `RunRowIterator` hooks (custom adapters)

Use [`writer.WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator)
when the sink is a built-in [`RowIteratorWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorWriter)
(`DelimitedWriter`, `JSONLWriter`, `SQLInsertWriter`). Use
[`writer.RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator)
with [`RowIteratorHooks`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks)
when the sink is not one of those writers—for example a legacy formatter, a
row transform into an app-owned type, or a presentation-specific export path
that should stay outside spanvalue.

`RunRowIterator` always calls `iter.Stop()`. Hook semantics (also on
[`RowIteratorHooks`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks)
godoc):

- **`PrepareMetadata`** runs once after the first `Next`, including when that
  call returns `iterator.Done` (zero data rows). It is not called when the first
  `Next` returns any other error.
- **`WriteRow`** runs for each data row.
- **`Finish`** runs only after all rows are consumed **without error**. It is
  **not** a `defer`-style cleanup when `PrepareMetadata` or `WriteRow` fails.
- On abort, `RunRowIterator` still returns `*RowIteratorResult` with whatever
  metadata and stats were available at the abort point.

Minimal adapter shape:

```go
result, err := writer.RunRowIterator(iter, writer.RowIteratorHooks{
	PrepareMetadata: func(md *spannerpb.ResultSetMetadata) error {
		return sink.Init(md)
	},
	WriteRow: func(row *spanner.Row) error {
		return sink.Write(row)
	},
	Finish: func(res *writer.RowIteratorResult) error {
		return sink.Close(res)
	},
})
```

Nil hook fields are skipped. For header-only or row-skip patterns, see
[Metadata-only finish after skipping rows](#metadata-only-finish-after-skipping-rows).

### Metadata-only finish after skipping rows

When the application consumes a `RowIterator` but does not write every row body
(for example a redacted or stats-only export path), metadata is still available
after `iterator.Done`. Register the row type and call `Flush` so delimited
writers emit a header-only CSV when columns were present but no data rows were
written:

```go
defer iter.Stop()

w, err := writer.NewDelimitedWriter(out, writer.Comma, writer.WithFormatter(cfg))
if err != nil {
	return err
}
for {
	_, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		break
	}
	if err != nil {
		return err
	}
	// discard row body
}
if err := w.PrepareRowType(iter.Metadata.GetRowType()); err != nil {
	return err
}
return w.Flush()
```

When every row is written normally, prefer [`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator)
instead of reimplementing the loop.

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
`WithColumnNames([])` at construction returns `writer.ErrMissingColumnNames`. See
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

### go-sql-spanner and GenericColumnValue export

[go-sql-spanner](https://github.com/googleapis/go-sql-spanner) apps often decode query
rows into `[]spanner.GenericColumnValue` (for example via proto decode options) and
export with `spanvalue` writers. `spanvalue` does **not** wrap `database/sql` or
`*sql.Rows`; keep a thin application loop (scan → GCV slice →
[`writer.WriteGCVs`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#DelimitedWriter.WriteGCVs)).

**Column names:** `database/sql` does not surface Spanner
`*spannerpb.ResultSetMetadata`; register columns with
[`writer.WithColumnNames`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WithColumnNames)
from your scan metadata (or `rows.Columns()` plus any unnamed-field policy).
When the app already holds `*spannerpb.ResultSetMetadata` (for example proto decode),
[`writer.WithMetadata`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WithMetadata)
is appropriate. For display headers outside the writer, use
[`spanvalue.ColumnNames`](https://pkg.go.dev/github.com/apstndb/spanvalue#ColumnNames)
on the **same** field list with the **same**
[`spanvalue.UnnamedFieldNamer`](https://pkg.go.dev/github.com/apstndb/spanvalue#UnnamedFieldNamer)
as [`writer.WithUnnamedFieldNamer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WithUnnamedFieldNamer).

**CSV / JSONL:** register schema and formatting at construction, stream rows, then
`Flush` (CSV may emit a header on zero-row `SELECT`; JSONL `Flush` is a no-op):

```go
namer := spanvalue.IndexedUnnamedFieldNamer
names := []string{"id", "name"} // same names passed to WithColumnNames
w, err := writer.NewCSVWriter(
	out,
	writer.WithColumnNames(names),
	writer.WithFormatter(spanvalue.SimpleFormatConfig()),
	writer.WithUnnamedFieldNamer(namer),
)
if err != nil {
	return err
}
defer rows.Close()
for rows.Next() {
	var gcvs []spanner.GenericColumnValue
	// decode the scanned row into gcvs
	if err := w.WriteGCVs(gcvs); err != nil {
		return err
	}
}
if err := rows.Err(); err != nil {
	return err
}
return w.Flush()
```

**Native Spanner client:** for `*spanner.RowIterator`, use
[`writer.WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator)
or [`writer.RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator)
instead of building GCV slices per row.

Metadata pseudo-rows, `NextResultSet` progression, and stats-only result sets stay
in the application (for example [spannersh](https://github.com/apstndb/spannersh)).

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
	w, err := writer.NewCSVWriter(out)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := w.WriteRow(row); err != nil {
			return err
		}
	}
	return w.Flush()
}
```

Quoted TSV uses the same CSV-style writer with a tab delimiter (`encoding/csv`
quoting: embedded tabs, quotes, and newlines in a field are escaped). For CSV output,
`NewCSVWriter` is a thin helper for `NewDelimitedWriter(out, writer.Comma)`. Pass
`writer.Comma` when using the generic delimited constructor for CSV output.
Delimiters must be non-zero valid runes other than `"`, `\r`, `\n`, or
`utf8.RuneError`.

```go
func writeTSV(out io.Writer, rows []*spanner.Row) error {
	w, err := writer.NewDelimitedWriter(out, '\t')
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := w.WriteRow(row); err != nil {
			return err
		}
	}
	return w.Flush()
}
```

Some CLIs expose a legacy **TAB** format that joins pre-formatted column strings
with `\t` and does not apply CSV-style quoting. That is not what
`NewDelimitedWriter(out, '\t')` emits. To keep raw tab-separated output while
still using spanvalue formatters, implement [`writer.Writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#Writer)
(or [`writer.RowIteratorWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorWriter)
when streaming via [`writer.WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator)): format each column, `strings.Join(fields, "\t")`,
then write the line.

JSONL output:

```go
func writeJSONL(out io.Writer, rows []*spanner.Row) error {
	w, err := writer.NewJSONLWriter(out)
	if err != nil {
		return err
	}
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
	w, err := writer.NewSQLInsertWriter(out, table)
	if err != nil {
		return err
	}
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
