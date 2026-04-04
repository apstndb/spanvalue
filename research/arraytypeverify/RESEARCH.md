# ARRAY `Type` metadata — observed behavior

Recorded from `research/arraytypeverify` (`go test -v -run TestAgainstEmulator` / `TestAgainstReal`).

| Case | Emulator: `Query` | Emulator: `AnalyzeQuery` | Real Spanner: `Query` | Real: `AnalyzeQuery` |
|------|-------------------|--------------------------|-------------------------|------------------------|
| `wellformed_ArrayValue_empty_defaults_to_ARRAY_INT64` | OK | Internal: `query plan unavailable` (emulator) | OK | OK |
| `wellformed_ArrayValueWithType_empty_INT64` | OK | Internal: `query plan unavailable` (emulator) | OK | OK |
| `malformed_ARRAY_missing_array_element_type` | `InvalidArgument`: `Array types must specify element type, found code: ARRAY` | same as Query | `InvalidArgument`: `The array_element_type field is required for ARRAYs.` | same as Query |

**Statement:** `SELECT @p AS p` with bound `spanner.GenericColumnValue`.

**Real instance (example):** `SPANNER_PROJECT_ID=gcpug-public-spanner`, `SPANNER_INSTANCE_ID=merpay-sponsored-instance`, `SPANNER_DATABASE_ID=apstndb-sampledb3` (read-only query; ADC required).

**Emulator image (spanemuboost default):** `gcr.io/cloud-spanner-emulator/emulator:1.5.50`.

## Conclusions (for #26 / #29)

- **Well-formed empty `ARRAY<INT64>`** from `gcvctor.ArrayValue()` (no args) and from `ArrayValueWithType(int64Elem, )` is accepted by **real Spanner** for both execution and `AnalyzeQuery`.
- **`Type` with `code = ARRAY` and no `array_element_type`** is **rejected** by both emulator and real Spanner with explicit `InvalidArgument` messages — there is no interoperable “ARRAY of unspecified element type” on the wire for this path.
- **Emulator** may return **Internal** / `query plan unavailable` on `AnalyzeQuery` even when `Query` succeeds; treat that as an emulator limitation when comparing to production.

## Follow-ups

- Keep building ARRAY types via `typector` / `gcvctor` helpers so `array_element_type` is always set.
- Optional: add pkg examples — not required for closing the research angle.
