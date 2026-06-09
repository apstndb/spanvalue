// Package dbsqlrows is experimental: APIs may change before a stable release.
//
// It streams [database/sql] query results into [github.com/apstndb/spanvalue/writer]
// using the GenericColumnValue slice export path, or into custom sinks via
// [SQLRowsHooks].
//
// Callers that use a Spanner database/sql driver (for example
// [github.com/googleapis/go-sql-spanner]) configure driver-specific options
// themselves; this package only iterates [*sql.Rows] once they are open.
//
// # Naming
//
// The name combines db (standard library [database/sql]) and sqlrows
// ([*sql.Rows] as input). For the native-client export path, use
// [github.com/apstndb/spanvalue/writer] ([writer.WriteRowIterator] on
// [*cloud.google.com/go/spanner.RowIterator]).
//
// # writer vs dbsqlrows
//
// | Path | Iterator | Row shape | spanvalue entry |
// |------|----------|-----------|-----------------|
// | Native client | [*spanner.RowIterator] | [*spanner.Row] | [writer.WriteRowIterator] |
// | database/sql driver | [*sql.Rows] | []spanner.GenericColumnValue | [writer.DelimitedWriter.WriteGCVs] |
// | dbsqlrows | [*sql.Rows] (caller-owned) | []spanner.GenericColumnValue | [RunRowsAtData] / [WriteRowsAtData] |
//
// dbsqlrows does not convert GCV slices to [*spanner.Row] for [writer.Writer.WriteRow].
//
// # Module layout
//
// Package path github.com/apstndb/spanvalue/dbsqlrows is part of the single
// github.com/apstndb/spanvalue module. The package does not import go-sql-spanner
// (or any database/sql driver). Optional one-shot helpers live in nested module
// github.com/apstndb/spanvalue/dbsqlrows/gospanner.
//
// # Goals
//
//   - Own the [*sql.Rows] loop: metadata pseudo-row → data rows → optional stats pseudo-row.
//   - Delegate csv/jsonl formatting to [writer.WriteGCVs] / [GCVStreamWriter.Flush].
//   - Expose [SQLRowsHooks] for custom sinks (table layout, drain-only) parallel to
//     [writer.RowIteratorHooks].
//   - Keep database/sql drivers out of spanvalue go.mod.
//
// # Non-goals
//
//   - Native [*spanner.RowIterator] export ([writer.WriteRowIterator]).
//   - String → GCV parsing, PostgreSQL table cells, or built-in ASCII table layout.
//   - Batch orchestration, SQL INSERT export, or owning db.QueryContext / driver ExecOptions.
//
// # API overview
//
// | Entry point | When to use |
// |-------------|-------------|
// | [WriteRows] | Open [*sql.Rows] at metadata pseudo-row; csv/jsonl via [GCVStreamWriter] |
// | [RunRows] / [RunRowsAtData] | Custom sinks via [SQLRowsHooks] |
// | [ReadMetadataAndAdvanceToData] | Metadata-first apps; advances cursor to data rows |
// | [WriteRowsAtData] | [RunRowsAtData] + [SQLRowsHooksFromGCVWriter] |
//
// Symmetry with writer:
//
// | writer | dbsqlrows |
// |--------|-----------|
// | [writer.RunRowIterator] | [RunRows] / [RunRowsAtData] |
// | [writer.RowIteratorHooks] | [SQLRowsHooks] |
// | [writer.RowIteratorHooksFromWriter] | [SQLRowsHooksFromGCVWriter] |
// | [writer.RowIteratorResult] | [SQLRowsResult] |
//
// [SQLRowsResult] carries Metadata when known on error paths (partial-result contract
// matching [writer.RowIteratorResult]). Stats are not consumed unless
// [SQLRowsConfig.ReadResultSetStats] is true; the iterator then advances with
// NextResultSet for multi-statement batches.
//
// An empty [SQLRowsHooks] value advances past data rows without per-row decode when
// WriteDataRow is nil (EXPLAIN / drain before stats). When WriteDataRow is set, the
// GCV slice passed to it is reused each row — copy or format before returning if
// the sink retains row data.
//
// # go-sql-spanner integration
//
// Option A (driver-agnostic): configure ExecOptions at query time, then pass
// open [*sql.Rows] to [WriteRows] or [RunRows]:
//
//	opts := spannerdriver.ExecOptions{
//	    DecodeOption:            spannerdriver.DecodeOptionProto,
//	    ReturnResultSetMetadata: true,
//	    ReturnResultSetStats:    false,
//	}
//	rows, err := db.QueryContext(ctx, q, opts)
//	// ...
//	result, err := dbsqlrows.WriteRows(rows, w, dbsqlrows.SQLRowsConfig{})
//
// Option B: nested module github.com/apstndb/spanvalue/dbsqlrows/gospanner provides
// DefaultExecOptions and QueryExport for one-shot query → csv/jsonl export when
// the app already depends on go-sql-spanner. It is a thin reference integration
// (ExecOptions + QueryContext + [WriteRows]); root go.mod still has no go-sql-spanner.
// Interactive shells, metadata-first batches, EXPLAIN, and per-query driver options
// (QueryMode, DirectExecuteQuery) should use Option A with app-owned ExecOptions
// instead — validated by [spannersh](https://github.com/apstndb/spannersh).
//
// # Stats: driver vs export
//
// A common REPL pattern: set ReturnResultSetStats true on the driver at
// QueryContext, keep [SQLRowsConfig.ReadResultSetStats] false during csv/jsonl/table
// export, then read the stats pseudo-row in application code after render. dbsqlrows
// leaves the cursor on the data result set until export completes or
// ReadResultSetStats is enabled.
//
// # Application patterns
//
// Metadata-first batch: [ReadMetadataAndAdvanceToData], render (table via
// [RunRowsAtData] + hooks), then read stats from rows or set ReadResultSetStats.
//
// Table sink: [RunRowsAtData] with [SQLRowsHooks.WithPrepareMetadata],
// [SQLRowsHooks.WithWriteDataRow], and [SQLRowsHooks.WithFinish] — apps own layout
// libraries and cell formatting.
//
// csv/jsonl: [WriteRowsAtData] with [writer.DelimitedGCVExportOptions] or
// [writer.JSONLGCVExportOptions] at writer construction.
//
// EXPLAIN / drain: [RunRowsAtData] with [NewSQLRowsHooks] and
// SQLRowsConfig.WithReadResultSetStats(true).
package dbsqlrows
