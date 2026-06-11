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
// Extended documentation, go-sql-spanner integration recipes (ExecOptions),
// and application patterns (REPL stats flow, table sinks):
// https://github.com/apstndb/spanvalue/blob/main/dbsqlrows/README.md
//
// # Naming
//
// The name combines db (standard library [database/sql]) and sqlrows
// ([*sql.Rows] as input). For the native-client export path, use
// [github.com/apstndb/spanvalue/writer]
// ([github.com/apstndb/spanvalue/writer.WriteRowIterator] on
// [*cloud.google.com/go/spanner.RowIterator]).
//
// # writer vs dbsqlrows
//
// Three export paths, by iterator and row shape:
//
//   - Native client: [*spanner.RowIterator] yielding [*spanner.Row];
//     spanvalue entry [github.com/apstndb/spanvalue/writer.WriteRowIterator].
//   - database/sql driver: [*sql.Rows] yielding []spanner.GenericColumnValue;
//     spanvalue entry [github.com/apstndb/spanvalue/writer.DelimitedWriter.WriteGCVs].
//   - dbsqlrows: [*sql.Rows] (caller-owned) yielding []spanner.GenericColumnValue;
//     spanvalue entry [RunRowsAtData] / [WriteRowsAtData].
//
// dbsqlrows does not convert GCV slices to [*spanner.Row] for
// [github.com/apstndb/spanvalue/writer.Writer.WriteRow].
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
//   - Delegate csv/jsonl formatting to [GCVStreamWriter.WriteGCVs] / [GCVStreamWriter.Flush].
//   - Expose [SQLRowsHooks] for custom sinks (table layout, drain-only) parallel to
//     [github.com/apstndb/spanvalue/writer.RowIteratorHooks].
//   - Keep database/sql drivers out of spanvalue go.mod.
//
// # Non-goals
//
//   - Native [*spanner.RowIterator] export
//     ([github.com/apstndb/spanvalue/writer.WriteRowIterator]).
//   - String → GCV parsing, PostgreSQL table cells, or built-in ASCII table layout.
//   - Batch orchestration, SQL INSERT export, or owning db.QueryContext / driver ExecOptions.
//
// # API overview
//
//   - [WriteRows]: open [*sql.Rows] at the metadata pseudo-row; csv/jsonl via
//     [GCVStreamWriter].
//   - [RunRows] / [RunRowsAtData]: custom sinks via [SQLRowsHooks].
//   - [ReadMetadataAndAdvanceToData]: metadata-first apps; advances cursor to
//     data rows.
//   - [WriteRowsAtData]: [RunRowsAtData] + [SQLRowsHooksFromGCVWriter].
//
// Symmetry with writer:
//
//   - [github.com/apstndb/spanvalue/writer.RunRowIterator] ↔ [RunRows] / [RunRowsAtData]
//   - [github.com/apstndb/spanvalue/writer.RowIteratorHooks] ↔ [SQLRowsHooks]
//   - [github.com/apstndb/spanvalue/writer.RowIteratorHooksFromWriter] ↔ [SQLRowsHooksFromGCVWriter]
//   - [github.com/apstndb/spanvalue/writer.RowIteratorResult] ↔ [SQLRowsResult]
//
// [SQLRowsResult] carries Metadata when known on error paths (partial-result contract
// matching [github.com/apstndb/spanvalue/writer.RowIteratorResult]). Stats are not
// consumed unless [SQLRowsConfig.ReadResultSetStats] is true; the iterator then
// advances with NextResultSet for multi-statement batches. [SQLRowsResult.RowsRead]
// follows [github.com/apstndb/spanvalue/writer.RowIteratorResult] RowsRead
// semantics: it counts data rows for which WriteDataRow returned nil and stays
// zero when WriteDataRow is nil, even though rows are still drained.
//
// [SQLRowsConfig.ReadResultSetStats] requires the driver to produce a stats
// pseudo result set (ReturnResultSetStats: true at QueryContext); otherwise the
// run fails with [ErrMissingStatsResultSet]. With driver stats disabled in a
// multi-statement batch, NextResultSet would otherwise land on the next
// statement's metadata result set and consume its pseudo-row before the scan
// fails, corrupting the batch cursor position.
//
// An empty [SQLRowsHooks] value advances past data rows without per-row decode when
// WriteDataRow is nil (EXPLAIN / drain before stats; RowsRead stays zero). When WriteDataRow is set, the
// GCV slice passed to it is reused each row — copy or format before returning if
// the sink retains row data.
package dbsqlrows
