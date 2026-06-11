# dbsqlrows

**Experimental** — APIs may change before a stable release.

Package documentation: [pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows)

Streams `database/sql` query results into [`writer`](../writer/README.md) using the `GenericColumnValue` slice export path, or into custom sinks via `SQLRowsHooks`. The package owns the `*sql.Rows` loop (metadata pseudo-row → data rows → optional stats pseudo-row) and keeps database/sql drivers out of the root `go.mod`. See the package godoc for the API contract; this README covers driver integration and application patterns.

## go-sql-spanner integration

Option A (driver-agnostic): configure `ExecOptions` at query time, then pass open `*sql.Rows` to [`WriteRows`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#WriteRows) or [`RunRows`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#RunRows):

```go
opts := spannerdriver.ExecOptions{
    DecodeOption:            spannerdriver.DecodeOptionProto,
    ReturnResultSetMetadata: true,
    ReturnResultSetStats:    false,
}
rows, err := db.QueryContext(ctx, q, opts)
// ...
result, err := dbsqlrows.WriteRows(rows, w, dbsqlrows.SQLRowsConfig{})
```

Option B: nested module [`gospanner/`](gospanner/README.md) provides `DefaultExecOptions` and `QueryExport` for one-shot query → csv/jsonl export when the app already depends on go-sql-spanner. It is a thin reference integration (`ExecOptions` + `QueryContext` + `WriteRows`); root `go.mod` still has no go-sql-spanner. Interactive shells, metadata-first batches, EXPLAIN, and per-query driver options (`QueryMode`, `DirectExecuteQuery`) should use Option A with app-owned `ExecOptions` instead — validated by [spannersh](https://github.com/apstndb/spannersh).

## Stats: driver vs export

A common REPL pattern: set `ReturnResultSetStats` true on the driver at `QueryContext`, keep [`SQLRowsConfig.ReadResultSetStats`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#SQLRowsConfig) false during csv/jsonl/table export, then read the stats pseudo-row in application code after render. dbsqlrows leaves the cursor on the data result set until export completes or `ReadResultSetStats` is enabled.

## Application patterns

- **Metadata-first batch:** [`ReadMetadataAndAdvanceToData`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#ReadMetadataAndAdvanceToData), render (table via [`RunRowsAtData`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#RunRowsAtData) + hooks), then read stats from rows or set `ReadResultSetStats`.
- **Table sink:** `RunRowsAtData` with [`SQLRowsHooks.WithPrepareMetadata`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#SQLRowsHooks.WithPrepareMetadata), `WithWriteDataRow`, and `WithFinish` — apps own layout libraries and cell formatting.
- **csv/jsonl:** [`WriteRowsAtData`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#WriteRowsAtData) with [`writer.DelimitedGCVExportOptions`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#DelimitedGCVExportOptions) or [`writer.JSONLGCVExportOptions`](https://pkg.go.dev/github.com/apstndb/spanvalue/writer#JSONLGCVExportOptions) at writer construction.
- **EXPLAIN / drain:** `RunRowsAtData` with [`NewSQLRowsHooks`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows#NewSQLRowsHooks) and `SQLRowsConfig.WithReadResultSetStats(true)`.

## Testing

```bash
go test ./dbsqlrows/...
# or from repo root: make check
```

Optional nested module [`gospanner/`](gospanner/README.md) (`QueryExport`, `DefaultExecOptions`).
