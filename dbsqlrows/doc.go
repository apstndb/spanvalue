// Package dbsqlrows streams [database/sql] query results from
// [github.com/googleapis/go-sql-spanner] into [github.com/apstndb/spanvalue/writer]
// using the GenericColumnValue slice export path.
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
// | go-sql-spanner | [*sql.Rows] | []spanner.GenericColumnValue | [writer.DelimitedWriter.WriteGCVs] |
// | dbsqlrows | [*sql.Rows] (owned by helper) | []spanner.GenericColumnValue | same WriteGCVs path |
//
// dbsqlrows does not convert GCV slices to [*spanner.Row] for [writer.Writer.WriteRow].
//
// # Module boundary
//
// go-sql-spanner is required only by this module's go.mod, not by the root
// github.com/apstndb/spanvalue module. Importers that never require dbsqlrows do
// not pull go-sql-spanner transitively.
//
// Extended goals, non-goals, and dependency diagram: see README.md in this directory.
package dbsqlrows
