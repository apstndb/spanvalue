// Package dbsqlrows streams [database/sql] query results into
// [github.com/apstndb/spanvalue/writer] using the GenericColumnValue slice export path.
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
// | dbsqlrows | [*sql.Rows] (caller-owned) | []spanner.GenericColumnValue | same WriteGCVs path |
//
// dbsqlrows does not convert GCV slices to [*spanner.Row] for [writer.Writer.WriteRow].
//
// # Module layout
//
// Package path github.com/apstndb/spanvalue/dbsqlrows is part of the single
// github.com/apstndb/spanvalue module. The package does not import go-sql-spanner
// (or any database/sql driver); callers configure the driver themselves. Optional
// one-shot helpers live in nested module github.com/apstndb/spanvalue/dbsqlrows/gospanner.
//
// For metadata-first flows (multi-statement batches, table render before CSV), use
// [ReadMetadataAndAdvanceToData] then [RunRowsAtData] with [SQLRowsHooks] or
// [ExportRowsAtData] for writer sinks; leave stats on rows unless
// [ExportConfig.ReadResultSetStats] is set.
//
// Custom sinks (ASCII table, observe-only) mirror [writer.RunRowIterator]: use
// [RunRows] or [RunRowsAtData] with [SQLRowsHooks]; csv/jsonl use
// [SQLRowsHooksFromGCVWriter] via [ExportRows] / [ExportRowsAtData].
//
// Extended goals, non-goals, and dependency diagram: see README.md in this directory.
package dbsqlrows
