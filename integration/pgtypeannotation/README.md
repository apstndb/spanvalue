# PostgreSQL dialect `TypeAnnotation` probe

This **nested Go module** exercises the Cloud Spanner Go client against a **PostgreSQL-dialect** database:

- Query parameters bound as `spanner.PGNumeric` / `spanner.PGJsonB` (encoded with `TypeAnnotation` on the wire).
- `RowIterator.Metadata.RowType` after the first `Next()` — column types should carry `PG_NUMERIC` / `PG_JSONB` annotations.

It does **not** import `spanvalue`; it is a standalone integration check for understanding PG + `TypeAnnotation` behavior.

SQL uses PostgreSQL placeholder syntax (`$1` with params keyed `p1`, …), matching `cloud.google.com/go/spanner` integration tests.

## Requirements

- Go 1.24+ (for `github.com/apstndb/spanemuboost`)
- Network access to Spanner API, **or** Docker (for the default emulator path via spanemuboost / testcontainers).

## Real Cloud Spanner

1. Enable the Spanner API and have an **existing instance** (this test does not create paid instances).
2. Application Default Credentials (`gcloud auth application-default login` or workload identity).
3. Run:

```sh
export SPANVALUE_PROJECT_ID=your-project
export SPANVALUE_INSTANCE_ID=your-instance
go test ./... -count=1 -v
```

The test creates a temporary PostgreSQL-dialect database and drops it in cleanup.

## Default: emulator via spanemuboost

With **no** `SPANVALUE_*` or `SPANNER_EMULATOR_HOST` env vars, the test runs the Cloud Spanner emulator inside Docker using [`github.com/apstndb/spanemuboost`](https://github.com/apstndb/spanemuboost) (`SetupEmulatorWithClients` + `DatabaseDialect_POSTGRESQL` + `WithRandomDatabaseID()`).

```sh
go test ./... -count=1 -v
```

Requires a working Docker (or compatible) runtime for testcontainers.

## Manual emulator (no Docker)

If you already run the emulator yourself:

```sh
export SPANNER_EMULATOR_HOST=localhost:9010
# e.g. docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
go test ./... -count=1 -v
```

If `CreateDatabase` with `DatabaseDialect_POSTGRESQL` fails, the test **skips** — use a recent emulator build with PostgreSQL support.

## What is asserted

- `ResultSetMetadata.row_type.fields[0].type.code` is `NUMERIC` or `JSON`.
- `type_annotation` is `PG_NUMERIC` or `PG_JSONB`.
- Row values round-trip into `spanner.PGNumeric` / `spanner.PGJsonB`.
