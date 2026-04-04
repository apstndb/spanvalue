# ARRAY `Type` metadata — observed behavior

Recorded from `research/arraytypeverify` (`go test -v -run TestAgainstEmulator` / `TestAgainstReal`).

**Mechanism:** For each case we run `SELECT @p AS p` with a bound `GenericColumnValue`, then:

| Step | API | Notes |
|------|-----|--------|
| Normal execution | `Query` | First `Next()` error or row |
| Plan + metadata | `QueryWithOptions(..., QueryOptions{Mode: PLAN})` | Drain iterator; read `RowIterator.Metadata` (JSON in test logs) |

`ResultSetMetadata` is defined in [result_set.proto](https://github.com/googleapis/googleapis/blob/master/google/spanner/v1/result_set.proto). For PLAN mode, **`undeclared_parameters`** describes inferred types for `@name` parameters when applicable.

## Summary table

| Case | Emulator: NORMAL `Query` | Emulator: PLAN `Metadata` | Real: NORMAL `Query` | Real: PLAN `Metadata` |
|------|----------------------------|-----------------------------|------------------------|-------------------------|
| `wellformed_ArrayValue_empty_defaults_to_ARRAY_INT64` | OK | `row_type` / `undeclared_parameters` show `ARRAY<INT64>` for `p` | OK | `row_type` shows `ARRAY<INT64>`; **`undeclared_parameters` empty** in sample run |
| `wellformed_ArrayValueWithType_empty_INT64` | OK | same as above | OK | same as above |
| `malformed_ARRAY_missing_array_element_type` | `InvalidArgument` (see log) | `<nil>` metadata (RPC fails) | `InvalidArgument` (see log) | `<nil>` metadata |

Exact error strings differ slightly between emulator and real (see test output).

**Real instance (example):** `SPANNER_PROJECT_ID=gcpug-public-spanner`, `SPANNER_INSTANCE_ID=merpay-sponsored-instance`, `SPANNER_DATABASE_ID=apstndb-sampledb3`.

**Emulator image (spanemuboost default):** `gcr.io/cloud-spanner-emulator/emulator:1.5.50`.

## Notes on `undeclared_parameters` (emulator vs real)

In sample runs, the **emulator** populated `undeclared_parameters.fields` with `p: ARRAY<INT64>` for well-formed values, while **real Spanner** returned `undeclared_parameters: {}` but still populated **`row_type.fields`** with the same `p` column type. Both are consistent for **row/column typing**; do not rely on `undeclared_parameters` alone being identical across backends.

## Conclusions (for #26 / #29)

- **Well-formed empty `ARRAY<INT64>`** from `gcvctor.ArrayValue()` (no args) and `ArrayValueWithType(int64Elem)` with no elements: accepted; PLAN **`ResultSetMetadata.row_type`** reflects `ARRAY<INT64>`.
- **`Type` with `code = ARRAY` and no `array_element_type`**: rejected with **`InvalidArgument`** before meaningful metadata is returned (`Metadata` nil on failure).
- Use **`QueryWithOptions` + PLAN** and **`RowIterator.Metadata`** (not only `AnalyzeQuery`) when documenting parameter / result typing from the plan path.

## Raw API: empty `params` list with **no** `param_types`

Using [`ExecuteSqlRequest`](https://pkg.go.dev/cloud.google.com/go/spanner/apiv1/spannerpb#ExecuteSqlRequest) directly (`raw_grpc_test.go`): `sql = SELECT @p AS p`, `params.p` = empty `list_value`, **`param_types` omitted**, `query_mode = PLAN`.

| Backend | Result |
|---------|--------|
| Emulator | `InvalidArgument`: e.g. `Could not parse list_value { } as INT64` |
| Real | `InvalidArgument`: e.g. `Invalid value for bind parameter p: Expected INT64.` |

So an **empty list** in `params` does **not** get inferred as `ARRAY<INT64>` without either:

- explicit **`param_types`**, or  
- a value shape that disambiguates (the high-level Go client sends **`Type`** on [`GenericColumnValue`](https://pkg.go.dev/cloud.google.com/go/spanner#GenericColumnValue), not JSON-only inference).

**Control:** same empty `list_value` with `param_types["p"] = ARRAY<INT64>` succeeds; metadata matches the typed `gcvctor` path.

## Follow-ups

- Keep building ARRAY types via `typector` / `gcvctor` helpers so `array_element_type` is always set.
