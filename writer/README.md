# writer

Stream Cloud Spanner query results to **CSV**, **quoted TSV**, **JSONL**, or **SQL INSERT** statements using [spanvalue](https://github.com/apstndb/spanvalue) formatters. The package sits beside the root formatter API: configure output with `spanvalue.FormatConfig` presets, then write rows through concrete writers or a shared `RowIterator` loop.

| Writer | Constructor | Notes |
|--------|-------------|--------|
| Delimited (CSV / TSV) | `NewCSVWriter`, `NewDelimitedWriter` | Uses `encoding/csv`; call `Flush` after the last row |
| JSONL | `NewJSONLWriter` | `Flush` is a no-op |
| SQL INSERT | `NewSQLInsertWriter` | `WithSQLBatchSize`, `WithSQLDialect`, `WithSQLInsertKind`; discard writer after a write error |

**Write paths:** `WriteRow` (`*spanner.Row`), `WriteStructValues` (`[]*structpb.Value` with registered field types), `WriteGCVs` (pre-built `GenericColumnValue` slices), or per-call `WriteValues`. Use [`Writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#Writer) for row-only adapters; use [`FlushWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#FlushWriter) when the adapter owns finalization.

## RowIterator

Production code with `*spanner.RowIterator` should treat metadata as **lazy**: `iter.Metadata` is populated after the first `Next()`. Do not pass `iter.Metadata` to `WithMetadata` at construction—it is still `nil`.

| Goal | API |
|------|-----|
| Built-in CSV / JSONL / SQL writer | [`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator) |
| Custom sink (legacy formatter, app-owned type) | [`RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator) with [`RowIteratorHooks`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks) |
| Manual loop (every query has ≥1 row) | `iter.Do` + `WriteRow`, or `PrepareRowType` after first `Next` then `WriteRow` + `return w.Flush()` |
| Zero-row `SELECT` (columns in metadata) | `WriteRowIterator` / `RunRowIterator`, or `PrepareRowType` after first `Next` then `Flush` |

[`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator) wires [`RowIteratorHooksFromWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooksFromWriter): `PrepareRowType` from metadata, `WriteRow` per data row, `Flush` in `Finish`. It always calls `iter.Stop()` and returns [`RowIteratorResult`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorResult) (metadata, query stats, [`RowsRead`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorResult.RowsRead)).

[`RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator) is the extension point for non-`RowIteratorWriter` sinks:

- **`PrepareMetadata`** — once after the first `Next`, including when that call returns `iterator.Done` (zero data rows). Not called when the first `Next` returns another error.
- **`WriteRow`** — each data row.
- **`Finish`** — only after all rows succeed without error (not `defer`-style cleanup on abort).

Construct hooks with [`NewRowIteratorHooks`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#NewRowIteratorHooks), [`RowIteratorHooks.WithPrepareMetadata`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks.WithPrepareMetadata) / `WithWriteRow` / `WithFinish`, or [`RowIteratorHooksFromWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooksFromWriter). Unexported fields prevent unkeyed composite literals from other packages.

**Decorators** (wrap an existing `RowIteratorHooks` value):

- [`WithRowOrdinal`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WithRowOrdinal) — 1-based row index for diagnostics
- [`ObserveWriteRow`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#ObserveWriteRow) — callback before each row
- [`AfterEachSuccessfulWriteRow`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#AfterEachSuccessfulWriteRow) — e.g. flush buffered I/O per row (not `SQLInsertWriter.Flush`)

`RowsRead` counts successful `WriteRow` hook calls; it differs from [`RowIteratorStats.RowCount`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorStats.RowCount) (Spanner DML semantics). Decorators that only observe rows may call [`MarkOmitRowsRead`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks.MarkOmitRowsRead) so side-effect-only `WriteRow` wrappers do not increment `RowsRead`.

```go
iter := txn.QueryWithStats(ctx, stmt)

w, err := writer.NewDelimitedWriter(out, '\t', writer.WithFormatter(cfg), writer.WithHeader(true))
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

```go
result, err := writer.RunRowIterator(iter, writer.NewRowIteratorHooks().
	WithPrepareMetadata(func(md *spannerpb.ResultSetMetadata) error {
		return sink.Init(md)
	}).
	WithWriteRow(func(row *spanner.Row) error {
		return sink.Write(row)
	}).
	WithFinish(func(res *writer.RowIteratorResult) error {
		return sink.Close(res)
	}))
```

When the app consumes `Next` but skips row bodies, register `PrepareRowType(iter.Metadata.GetRowType())` after the loop, then `Flush` for a header-only delimited export—see package godoc.

**Manual loops:** propagate `Flush()` errors (`return w.Flush()`, not `defer w.Flush()`). When driving `Next` yourself, `defer iter.Stop()`.

## Schema registration

| Situation | Registration |
|-----------|----------------|
| Names only | `WithColumnNames` / `PrepareColumnNames` (≥1 name) |
| Names + types | `WithRowType` / `PrepareRowType` / `WithMetadata` when metadata is already known |
| Zero columns (DML without `THEN RETURN`) | `PrepareRowType(nil)` or empty row type—not `PrepareColumnNames([])` |
| Zero-row `SELECT` | `PrepareRowType` after first `Next`, then `Flush` for header-only CSV |

Without registration and with no row written, `Flush` / `WriteHeader` return `ErrMissingColumnNames`. Registered empty schema (`len(names)==0`) is valid: `Flush` writes nothing.

## go-sql-spanner and GCV slices

`spanvalue` does **not** wrap `database/sql` or `*sql.Rows`. Apps decode rows to `[]spanner.GenericColumnValue` and call `WriteGCVs`, registering columns with `WithColumnNames` or `WithMetadata` when available.

**Adoption boundary and recipes** (scan loop, `DelimitedGCVExportOptions`, native client vs GCV path): [root README — go-sql-spanner and GenericColumnValue export](../README.md#go-sql-spanner-and-genericcolumnvalue-export).

Match out-of-band headers with [`spanvalue.ColumnNames`](https://pkg.go.dev/github.com/apstndb/spanvalue#ColumnNames) and the same `UnnamedFieldNamer` as `WithUnnamedFieldNamer`.

## Formats and edge cases

- **Quoted TSV:** `NewDelimitedWriter(out, '\t')` uses CSV escaping, not raw tab joins. Legacy raw TAB: implement `Writer` or `RowIteratorWriter` and join formatted columns with `'\t'`.
- **SQL INSERT:** GoogleSQL quoting by default; `WithSQLDialect` for PostgreSQL identifiers. After any write error from `SQLInsertWriter`, discard the writer.
- **Delimited vs JSONL vs SQL:** spanvalue formats each cell; encodings differ afterward. One-shot helpers: `FormatDelimitedRow`, `FormatJSONLRow`, `RowData`.

## Future module split

`writer` may move to a **separate Go module** in a later release while staying in this repository path during transition. For **v0.5.x**, treat exported `writer` APIs as stable within semver pre-release rules; importers should pin versions and run their own export golden tests when upgrading. Formatting behavior remains in the root `spanvalue` module—only the import path would change if split.
