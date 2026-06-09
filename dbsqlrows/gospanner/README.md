# gospanner

Optional nested Go module — **reference integration** for
[go-sql-spanner](https://github.com/googleapis/go-sql-spanner) + core
[`dbsqlrows`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows).

```text
import "github.com/apstndb/spanvalue/dbsqlrows/gospanner"
```

The root [`github.com/apstndb/spanvalue`](../../) module still has **no**
go-sql-spanner dependency.

## Why this module exists

| Audience | Use |
|----------|-----|
| One-shot CLIs / scripts | `QueryExport` → csv/jsonl without boilerplate |
| New adopters | `DefaultExecOptions` documents proto decode + metadata pseudo-row |
| Interactive shells (e.g. [spannersh](https://github.com/apstndb/spannersh)) | **Do not use** — app-owned `ExecOptions` + core `dbsqlrows` primitives |

spannersh validated that metadata-first REPLs need QueryMode, multi-statement batches,
and stats-after-render; those stay in the application with Option A (core cookbook).
`gospanner` is intentionally narrow — not a gap when shells skip it.

## Module

```bash
go get github.com/apstndb/spanvalue/dbsqlrows/gospanner@v0.6.0
```

Local development in this repository uses `replace github.com/apstndb/spanvalue => ../..` in [`go.mod`](go.mod). That directive is dev-only (ignored by downstream `go get`); consumers need a published `github.com/apstndb/spanvalue` v0.6.0 or newer—the first release that includes `dbsqlrows`.

This nested module targets **Go 1.25** (required by go-sql-spanner v1.25.1). The root `spanvalue` module remains on Go 1.23 per [AGENTS.md](../../AGENTS.md).

## API

| Function | Role |
|----------|------|
| [`DefaultExecOptions`](export.go) | Proto decode + metadata pseudo-row; stats left for caller |
| [`QueryExport`](export.go) | `QueryContext` + [`dbsqlrows.WriteRows`](../export.go) |
| [`QueryExportWithOptions`](export.go) | Same with explicit `ExecOptions` |

## Example

```go
import (
    "context"
    "database/sql"
    "os"

    "github.com/apstndb/spanvalue/dbsqlrows"
    "github.com/apstndb/spanvalue/dbsqlrows/gospanner"
    "github.com/apstndb/spanvalue/writer"
)

w, err := writer.NewCSVWriter(os.Stdout, writer.WithHeader(true))
if err != nil {
    return err
}
result, err := gospanner.QueryExport(
    ctx, db, "SELECT id, name FROM Users", nil, w, dbsqlrows.SQLRowsConfig{},
)
if err != nil {
    return err
}
_ = result.Metadata
```

For metadata-first, table, EXPLAIN, or multi-statement flows, use
[`dbsqlrows`](https://pkg.go.dev/github.com/apstndb/spanvalue/dbsqlrows) directly
and configure `ExecOptions` in app code (see package documentation). Typical REPL
pattern: driver `ReturnResultSetStats: true`, export `ReadResultSetStats: false`,
read stats pseudo-row after render.

## Development

```bash
cd dbsqlrows/gospanner && go test ./...
```

Root `make check` does not run this nested module. CI runs `go test ./...` here
with Go 1.25 (see `.github/workflows/go.yml`).

## Related

- [#178](https://github.com/apstndb/spanvalue/issues/178) — design
- [#190](https://github.com/apstndb/spanvalue/pull/190) — implementation PR
