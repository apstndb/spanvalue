# PostgreSQL dialect `TypeAnnotation` probe

This **nested Go module** exercises the Cloud Spanner Go client against a **PostgreSQL-dialect** database:

- Query parameters bound as `spanner.PGNumeric` / `spanner.PGJsonB` (encoded with `TypeAnnotation` on the wire).
- `RowIterator.Metadata.RowType` after the first `Next()` — column types should carry `PG_NUMERIC` / `PG_JSONB` annotations.

It does **not** import `spanvalue`; it is a standalone integration check for understanding PG + `TypeAnnotation` behavior.

## Requirements

- Go 1.23+
- Network access to Spanner API or a local emulator.

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

## Emulator

```sh
export SPANNER_EMULATOR_HOST=localhost:9010
# Start the emulator separately, e.g.:
# docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
go test ./... -count=1 -v
```

If `CreateDatabase` with `DatabaseDialect_POSTGRESQL` fails, the test **skips** — many emulator builds still omit full PostgreSQL support (the same reason `cloud.google.com/go/spanner` integration tests skip PG on the emulator).

## What is asserted

- `ResultSetMetadata.row_type.fields[0].type.code` is `NUMERIC` or `JSON`.
- `type_annotation` is `PG_NUMERIC` or `PG_JSONB`.
- Row values round-trip into `spanner.PGNumeric` / `spanner.PGJsonB`.
