# spanvalue

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spanvalue.svg)](https://pkg.go.dev/github.com/apstndb/spanvalue)

Helpers for working with Cloud Spanner’s [`spanner.GenericColumnValue`](https://pkg.go.dev/cloud.google.com/go/spanner#GenericColumnValue) and related client types: **format** values to text (literals, JSON, CLI-style output) and **construct** values from Go types.

| Package | Role |
|--------|------|
| [`github.com/apstndb/spanvalue`](https://pkg.go.dev/github.com/apstndb/spanvalue) | Format `spanner.GenericColumnValue` and `*spanner.Row` using [`FormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatConfig) and presets such as [`LiteralFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#LiteralFormatConfig), [`JSONFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#JSONFormatConfig), [`SpannerCLICompatibleFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#SpannerCLICompatibleFormatConfig). |
| [`github.com/apstndb/spanvalue/gcvctor`](https://pkg.go.dev/github.com/apstndb/spanvalue/gcvctor) | Build `spanner.GenericColumnValue` (scalars, `ARRAY`, `STRUCT`, typed nulls). Types are often composed with [`github.com/apstndb/spantype/typector`](https://pkg.go.dev/github.com/apstndb/spantype/typector). |
| [`github.com/apstndb/spanvalue/protofmt`](https://pkg.go.dev/github.com/apstndb/spanvalue/protofmt) | Opt-in descriptor-aware PROTO and ENUM display plugins for [`FormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatConfig). |
| [`github.com/apstndb/spanvalue/writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer) | Stream Spanner rows to CSV, TSV, JSONL, or SQL INSERT ([writer/README.md](writer/README.md)). |
| [`github.com/apstndb/spanvalue/dbsqlrows`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows) | Driver-agnostic `database/sql` export adapter ([dbsqlrows/README.md](dbsqlrows/README.md)); callers supply go-sql-spanner (or another driver). |

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

Package [`writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer) streams
`*spanner.Row`, structpb cells, or `[]spanner.GenericColumnValue` to CSV, quoted TSV,
JSONL, or SQL INSERT using spanvalue formatters. **[writer/README.md](writer/README.md)**
covers `RowIterator` lifecycle ([`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator),
[`RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator),
hooks and decorators, `RowsRead`), schema registration, format edge cases, and v0.5.x
expectations if `writer` becomes a separate module.

When every query returns at least one row, `iter.Do` plus `WriteRow` and `Flush` is enough
(the first row registers column names). When a `SELECT` may return zero rows but metadata
still lists columns, use `WriteRowIterator` (see writer README).

```go
iter := txn.Query(ctx, stmt)

w, err := writer.NewCSVWriter(out, writer.WithFormatter(cfg))
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

### go-sql-spanner and GenericColumnValue export

Writer package details (GCV export options, `RowIterator` vs `WriteGCVs`): [writer/README.md](writer/README.md).

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
`Flush` (CSV may emit a header on zero-row `SELECT`; JSONL `Flush` is a no-op).

When the app already holds `*spannerpb.ResultSetMetadata` (for example from proto
decode), pass metadata, formatter, and namer together with
[`writer.DelimitedGCVExportOptions`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#DelimitedGCVExportOptions)
or [`writer.JSONLGCVExportOptions`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#JSONLGCVExportOptions)
(nil arguments are skipped):

```go
w, err := writer.NewCSVWriter(out, writer.DelimitedGCVExportOptions(
	metadata,
	spanvalue.SimpleFormatConfig(),
	spanvalue.IndexedUnnamedFieldNamer,
)...)
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

If you only have column names from `database/sql` (no `ResultSetMetadata`), use
separate `With*` options instead:

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
For display paths where protobuf descriptors are available, prepend opt-in
[`protofmt`](https://pkg.go.dev/github.com/apstndb/spanvalue/protofmt) plugins to a cloned
formatter. These plugins render PROTO values as protobuf text and ENUM values
as names; they are display-oriented and do not replace descriptor-free SQL
literal output such as `FormatProtoAsCast` / `FormatEnumAsCast`. Descriptor
loading and compilation stay in the application. If you enable multiline
prototext, nested ARRAY/STRUCT cells and delimited-output fields can contain
embedded newlines.
Delimited, JSONL, and SQL encodings differ after
spanvalue formats each column; see [writer/README.md](writer/README.md). For
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
