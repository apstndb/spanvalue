# writer

Stream Cloud Spanner query results to **CSV**, **quoted TSV**, **JSONL**, or **SQL INSERT** statements using [spanvalue](https://github.com/apstndb/spanvalue) formatters. The package sits beside the root formatter API: configure output with `spanvalue.FormatConfig` presets, then write rows through concrete writers or a shared `RowIterator` loop.

| Writer | Constructor | Notes |
|--------|-------------|--------|
| Delimited (CSV / TSV) | `NewCSVWriter`, `NewDelimitedWriter` | Uses `encoding/csv`; call `Flush` after the last row, or `WithFlushEachRow` for incremental output |
| JSONL | `NewJSONLWriter` | `Flush` is a no-op |
| SQL INSERT | `NewSQLInsertWriter` | `WithSQLBatchSize`, `WithSQLDialect`, `WithSQLInsertKind`; empty table name rejected at construction; qualified names with empty segments on first write; discard writer after a write error |

**Write paths:** `WriteRow` (`*spanner.Row`), `WriteStructValues` (`[]*structpb.Value` with registered field types), `WriteGCVs` (pre-built `GenericColumnValue` slices), or per-call `WriteValues`. Use [`Writer`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#Writer) for row-only adapters; use [`FlushWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#FlushWriter) when the adapter owns finalization.

**Formatter:** set [`WithFormatter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WithFormatter) at construction; inspect the effective preset with [`FormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#DelimitedWriter.FormatConfig) on delimited, JSONL, or SQL INSERT writers (fields are not exported in v0.5.0+). Writers do not call [`spanvalue.FormatConfig.Validate`](https://pkg.go.dev/github.com/apstndb/spanvalue#FormatConfig.Validate) on the supplied config—validate hand-built formatters before construction (see root README).

## RowIterator

Production code with `*spanner.RowIterator` should treat metadata as **lazy**: `iter.Metadata` is populated after the first `Next()`. Do not pass `iter.Metadata` to `WithMetadata` at construction—it is still `nil`.

| Goal | API |
|------|-----|
| Built-in CSV / JSONL / SQL writer | [`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator) |
| Custom sink (legacy formatter, app-owned type) | [`RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator) with [`RowIteratorHooks`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks) |
| Manual loop (every query has ≥1 row) | `iter.Do` + `WriteRow`, or `PrepareRowType` after first `Next` then `WriteRow` + `return w.Flush()` |
| Zero-row `SELECT` (columns in metadata) | `WriteRowIterator` / `RunRowIterator`, or `PrepareRowType` after first `Next` then `Flush` |

### Iterator ownership

[`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator) and [`RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator) **own** the iterator passed in: they consume it, call `iter.Stop()`, and return [`RowIteratorResult`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorResult) for post-run metadata and stats.

Prefer passing a newly created iterator directly—do not bind it and `defer iter.Stop()` at the call site:

```go
result, err := writer.WriteRowIterator(txn.Query(ctx, stmt), w)
```

After the helper returns, use `result.Metadata`, `result.Stats`, and `result.RowsRead`. Do not read `iter.Metadata`, `iter.QueryStats`, `iter.RowCount`, or `iter.QueryPlan` on the transferred iterator; reading those fields after `Stop()` does not panic in current Spanner client releases, but the helper owns lifecycle and the returned result is the supported API.

**Manual loops** differ: bind `iter := txn.Query(ctx, stmt)`, `defer iter.Stop()`, consume rows yourself, and read iterator fields only after reaching `iterator.Done`.

[`WriteRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WriteRowIterator) wires [`RowIteratorHooksFromWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooksFromWriter): `PrepareRowType` from metadata, `WriteRow` per data row, `Flush` in `Finish`.

[`RunRowIterator`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RunRowIterator) is the extension point for non-`RowIteratorWriter` sinks:

- **`PrepareMetadata`** — once after the first `Next`, including when that call returns `iterator.Done` (zero data rows). Not called when the first `Next` returns another error.
- **`WriteRow`** — each data row.
- **`Finish`** — only after all rows succeed without error (not `defer`-style cleanup on abort).

Construct hooks with [`NewRowIteratorHooks`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#NewRowIteratorHooks), [`RowIteratorHooks.WithPrepareMetadata`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks.WithPrepareMetadata) / `WithWriteRow` / `WithFinish`, or [`RowIteratorHooksFromWriter`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooksFromWriter). Unexported fields prevent unkeyed composite literals from other packages.

**Decorators** (wrap an existing `RowIteratorHooks` value):

- [`WithRowOrdinal`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WithRowOrdinal) — 1-based row index for diagnostics
- [`ObserveWriteRow`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#ObserveWriteRow) — callback before each row
- [`AfterEachSuccessfulWriteRow`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#AfterEachSuccessfulWriteRow) — custom per-row hooks (for delimited streaming without type assertions, prefer [`WithFlushEachRow`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#WithFlushEachRow); not `SQLInsertWriter.Flush`)

`RowsRead` counts successful `WriteRow` hook calls; it differs from [`RowIteratorStats.RowCount`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorStats.RowCount) (Spanner DML semantics). Decorators that only observe rows may call [`MarkOmitRowsRead`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#RowIteratorHooks.MarkOmitRowsRead) so side-effect-only `WriteRow` wrappers do not increment `RowsRead`.

```go
w, err := writer.NewDelimitedWriter(out, '\t', writer.WithFormatter(cfg), writer.WithHeader(true))
if err != nil {
	return err
}
result, err := writer.WriteRowIterator(txn.QueryWithStats(ctx, stmt), w)
if err != nil {
	return err
}
_ = result.Metadata
_ = result.Stats.QueryStats
```

```go
result, err := writer.RunRowIterator(txn.Query(ctx, stmt), writer.NewRowIteratorHooks().
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

For complex adapters with several app-owned sinks, keep the state in a small
local helper and expose that helper through hooks:

```go
type exportSink struct {
	formatter appFormatter
	metrics   appMetrics
}

sink := exportSink{
	formatter: appFormatterImpl{},
	metrics:   appMetricsImpl{},
}
hooks := writer.NewRowIteratorHooks().
	WithPrepareMetadata(func(md *spannerpb.ResultSetMetadata) error {
		return sink.formatter.Prepare(md)
	}).
	WithWriteRow(func(row *spanner.Row) error {
		if err := sink.metrics.ObserveRow(); err != nil {
			return err
		}
		return sink.formatter.Write(row)
	}).
	WithFinish(func(result *writer.RowIteratorResult) error {
		return sink.formatter.Finish(result)
	})

result, err := writer.RunRowIterator(txn.Query(ctx, stmt), hooks)
```

This keeps transformation, metrics, and finalization policy in the application
while still using `RunRowIterator` for iterator ownership, metadata timing, and
result collection. Prefer this pattern until a shared composition helper removes
enough real downstream boilerplate to justify another writer API.

When the app consumes `Next` but skips row bodies, register `PrepareRowType(iter.Metadata.GetRowType())` after the loop, then `Flush` for a header-only delimited export—see package godoc.

**Manual loops:** bind the iterator, `defer iter.Stop()`, propagate `Flush()` errors (`return w.Flush()`, not `defer w.Flush()`), and read metadata or stats only after consuming to `iterator.Done`.

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

- **Duplicate column headers:** CSV/TSV header rows follow resolved [`spanvalue.ColumnNames`](https://pkg.go.dev/github.com/apstndb/spanvalue#ColumnNames) output, **including duplicate explicit aliases** (for example `SELECT 1 AS a, 2 AS a` → header `a,a`). RFC 4180 permits repeated header names; consumers that require unique headers must disambiguate in the application. JSONL object keys from duplicate aliases are a separate concern—see [`spanvalue.NewJSONObjectStructFormatter`](https://pkg.go.dev/github.com/apstndb/spanvalue#NewJSONObjectStructFormatter) and root JSON row docs for duplicate-key behavior.
- **Quoted TSV:** `NewDelimitedWriter(out, '\t')` uses CSV escaping, not raw tab joins. Legacy raw TAB: implement `Writer` or `RowIteratorWriter` and join formatted columns with `'\t'`.
- **SQL INSERT:** GoogleSQL quoting by default; `WithSQLDialect` for PostgreSQL identifiers. `NewSQLInsertWriter` rejects an empty table name at construction (whitespace-only per strings.TrimSpace) and qualified names with empty segments on the first write. After any write error from `SQLInsertWriter`, discard the writer.
- **Delimited vs JSONL vs SQL:** spanvalue formats each cell; encodings differ afterward. One-shot helpers: `FormatDelimitedRow`, `FormatJSONLRow`, `RowData`.

## Future module split

`writer` may move to a **separate Go module** in a later release while staying in this repository path during transition. For **v0.5.x**, treat exported `writer` APIs as stable within semver pre-release rules; importers should pin versions and run their own export golden tests when upgrading. Formatting behavior remains in the root `spanvalue` module—only the import path would change if split.
