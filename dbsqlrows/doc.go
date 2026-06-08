// Package dbsqlrows streams [database/sql] query results into
// [github.com/apstndb/spanvalue/writer] using the GenericColumnValue slice export path.
//
// Callers that use a Spanner database/sql driver (for example
// [github.com/googleapis/go-sql-spanner]) configure driver-specific options
// themselves; this package only iterates [*sql.Rows] once they are open.
//
// # Naming
//
// The module name combines db (standard library [database/sql]), sqlrows
// ([*sql.Rows] as input), and lives under the spanvalue repository. It is
// intentionally explicit: nine characters, comparable to spanvalue, and distinct
// from the native-client path in [github.com/apstndb/spanvalue/writer] that drives
// [*cloud.google.com/go/spanner.RowIterator] via [writer.WriteRowIterator].
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
// # Module boundary
//
// This optional module lives under github.com/apstndb/spanvalue/dbsqlrows with its
// own go.mod. It does not add go-sql-spanner (or any database/sql driver) to the
// root github.com/apstndb/spanvalue module.
//
// For metadata-first flows (multi-statement batches, table render before CSV), use
// [ReadMetadataAndAdvanceToData] then [ExportRowsAtData] or app-owned rendering;
// leave stats on rows unless [ExportConfig.ReadResultSetStats] is set.
//
// Extended goals, non-goals, and dependency diagram: see README.md in this directory.
package dbsqlrows
