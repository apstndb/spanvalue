// Package gospanner wires [github.com/googleapis/go-sql-spanner] query execution
// to [github.com/apstndb/spanvalue/dbsqlrows] export helpers.
//
// Import this nested module only when the application already depends on
// go-sql-spanner and wants a one-shot QueryContext + [dbsqlrows.ExportRows] helper.
// The root github.com/apstndb/spanvalue module does not require go-sql-spanner.
//
// # When to use gospanner
//
//   - Small tools or scripts: single SELECT → CSV/JSONL via [QueryExport].
//   - Reference integration: shows recommended ExecOptions for proto-decoded GCV
//     export with a metadata pseudo-row ([DefaultExecOptions]).
//
// # When to use core dbsqlrows instead
//
//   - Metadata-first or multi-statement batches ([dbsqlrows.ReadMetadataAndAdvanceToData]).
//   - Table or custom sinks ([dbsqlrows.RunRowsAtData] + [dbsqlrows.SQLRowsHooks]).
//   - Per-query driver options (QueryMode PLAN/PROFILE, DirectExecuteQuery).
//   - Stats read after render with driver ReturnResultSetStats true and export
//     ReadResultSetStats false (REPL pattern; see [dbsqlrows] package documentation).
//
// [DefaultExecOptions] sets ReturnResultSetStats false — appropriate for simple
// export CLIs, not for shells that render first then show execution summaries.
//
// See README.md in this directory.
package gospanner
