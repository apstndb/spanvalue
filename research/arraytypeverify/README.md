# arraytypeverify

Disposable `go test` harness for **Issue #29** / **#26**: observe how Cloud Spanner (emulator vs real) handles `spanner.GenericColumnValue` when `Type` is `ARRAY` but **`array_element_type` is unset**, and compare with well-formed `gcvctor` values (including zero-argument `ArrayValue()` → empty `ARRAY<INT64>`).

This module is **not** part of the main `spanvalue` module; it lives under `research/` so CI does not require Docker.

## Prerequisites

- **Emulator path**: Docker (for [spanemuboost](https://github.com/apstndb/spanemuboost) / testcontainers).
- **Real instance path**: Application Default Credentials with permission to run read-only queries on the target database.

## Emulator

From this directory:

```bash
go test -v -run TestAgainstEmulator ./...
```

## Real Spanner (example: public sample)

Unset the emulator host, then set project / instance / database (example values suggested for community verification):

```bash
unset SPANNER_EMULATOR_HOST
export SPANNER_PROJECT_ID=gcpug-public-spanner
export SPANNER_INSTANCE_ID=merpay-sponsored-instance
export SPANNER_DATABASE_ID=apstndb-sampledb3
go test -v -run TestAgainstReal ./...
```

## What gets exercised

For each case, the test runs:

1. **`ReadOnlyTransaction.Query`** — `ExecuteSql` in **NORMAL** mode (first row of `SELECT @p AS p` with the bound `GenericColumnValue`).
2. **`ReadOnlyTransaction.QueryWithOptions`** with **`ExecuteSqlRequest.QueryMode` = `PLAN`** (same SQL and params). The iterator is drained; then **`RowIterator.Metadata`** (`[google.spanner.v1.ResultSetMetadata]`) is logged as JSON. In PLAN mode this includes **`row_type`**, **`undeclared_parameters`** (when populated), and **`transaction`**. When the backend attaches a plan to the stream, **`RowIterator.QueryPlan`** may be non-nil.

Prefer this over **`AnalyzeQuery`**: it uses the same PLAN RPC path but exposes **full `ResultSetMetadata`**, which is what documents parameter typing for undeclared names.

Cases include:

- `gcvctor.ArrayValue()` with no arguments (empty `ARRAY<INT64>`).
- `gcvctor.ArrayValueWithType(INT64)` with no elements.
- A deliberately malformed `Type`: `code = ARRAY` and no `array_element_type`, with an empty list value.

## Raw gRPC (`ExecuteSqlRequest` without going through `cloud.google.com/go/spanner` Statement)

[`ExecuteSqlRequest`](https://pkg.go.dev/cloud.google.com/go/spanner/apiv1/spannerpb#ExecuteSqlRequest) has optional **`param_types`**. The field comment states that Spanner cannot always infer the SQL type from JSON-like **`params`** alone (e.g. `BYTES` vs `STRING`), so callers may supply `param_types` for some or all parameters.

Tests in **`raw_grpc_test.go`** use the gapic client ([`spanner.NewClient`](https://pkg.go.dev/cloud.google.com/go/spanner/apiv1#NewClient) from `cloud.google.com/go/spanner/apiv1`) and call **`ExecuteStreamingSql`** with:

- **`params`**: `@p` = empty protobuf `list_value` (`structpb` empty list).
- **`param_types`**: omitted vs explicitly `ARRAY<INT64>`.

This checks whether an **empty list with no `param_types`** is enough for the server to treat `@p` as `ARRAY<INT64>` (it is not, in our runs — see `RESEARCH.md`).

```bash
go test -v -run TestRawGRPC ./...
```

## Recording results

After running both backends, paste concise outcomes into `RESEARCH.md` and use that to update package docs (#26) and the issue #29 write-up.
