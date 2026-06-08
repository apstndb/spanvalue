# gospanner

Optional nested Go module for apps that already use
[go-sql-spanner](https://github.com/googleapis/go-sql-spanner). It wires driver
`ExecOptions` and `QueryContext` to the driver-agnostic
[`dbsqlrows`](../README.md) export loop.

```text
import "github.com/apstndb/spanvalue/dbsqlrows/gospanner"
```

The root [`github.com/apstndb/spanvalue`](../../) module still has **no**
go-sql-spanner dependency. Add this module only when you need one-shot query +
export helpers.

## Module

```bash
go get github.com/apstndb/spanvalue/dbsqlrows/gospanner@v0.6.0
```

Local development in this repository uses `replace github.com/apstndb/spanvalue => ../..` in [`go.mod`](go.mod).

## API

| Function | Role |
|----------|------|
| [`DefaultExecOptions`](export.go) | Proto decode + metadata pseudo-row; stats left for caller |
| [`QueryExport`](export.go) | `QueryContext` + [`dbsqlrows.ExportRows`](../export.go) |
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
    ctx, db, "SELECT id, name FROM Users", nil, w, dbsqlrows.ExportConfig{},
)
if err != nil {
    return err
}
_ = result.Metadata
```

For metadata-first or multi-statement flows, use [`dbsqlrows`](../README.md)
primitives directly and configure `ExecOptions` in app code (see core README
cookbook).

## Development

```bash
cd dbsqlrows/gospanner && go test ./...
```

Root `make check` does not run this nested module; CI for gospanner is a
follow-up.

## Related

- [#178](https://github.com/apstndb/spanvalue/issues/178) — design
- [#190](https://github.com/apstndb/spanvalue/pull/190) — implementation PR
