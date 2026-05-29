# Writer schema registration and row input shapes

Design notes from a review of how `github.com/apstndb/spanvalue/writer` and the root
`spanvalue` package use `ResultSetMetadata`, column names, and
`spanner.GenericColumnValue` (GCV). This document records the intended mental model,
current behavior, gaps, and proposed API directions. It is not an implementation
checklist.

## Problem statement

Callers streaming formatted rows often have one of these inputs:

- `*sppb.ResultSetMetadata` from `ExecuteSql`
- `*sppb.StructType` (row type) with `[]*sppb.StructType_Field`
- `[]string` column names only
- `[]spanner.GenericColumnValue` per row (type + value per column)
- `[]*structpb.Value` per row when schema is fixed

Today, **formatting uses types and values from GCV only**. Metadata contributes
**column names** for labeled output (CSV header, JSON keys, SQL column lists). That
split is easy to miss because `WithMetadata` and `Prepare` accept a large Spanner
message while the implementation reads only `metadata.row_type.fields[].name`.

When callers already have GCVs but not metadata, they may fabricate a dummy
`ResultSetMetadata` (see tests: `metadataWithColumnNames` fills placeholder
`Type` fields that production code never reads). That adapter noise suggests a
missing first-class schema API.

## Logical model for one row

For `n` columns, a row needs:

1. **Cell payload**: `(Type, Value)` per column — bundled as `spanner.GenericColumnValue`
2. **Labels** (optional): column names — for JSONL, delimited headers, `INSERT` column lists

Equivalent decompositions:

| Representation | Names | Types | Values |
|----------------|-------|-------|--------|
| `columnNames` + `[]GCV` | external slice | each `gcv.Type` | each `gcv.Value` |
| `rowType.Fields` + `[]*structpb.Value` | `fields[i].Name` | `fields[i].Type` | `values[i]` |
| `ResultSetMetadata` | via `row_type.fields[].name` | unused by spanvalue today | via GCV or `structpb.Value` |

`FormatRowColumns` documents that names are only used for shape checks and labeling;
formatted text comes from GCVs:

```go
// FormatRowColumns formats a row represented as column names plus GCV values.
// The column names are validated for shape compatibility, but the formatted cell
// values come from the GCVs themselves.
```

`FormatColumn` dispatches on `value.Type` (and `value.Value`), including nested
ARRAY/STRUCT using the type tree inside each GCV.

### What `ResultSetMetadata` is here

- **Spanner API**: metadata includes `row_type` plus other fields (e.g. transaction,
  undeclared parameters).
- **spanvalue writer today**: effectively a **row-type name carrier** — same information
  as `metadata.GetRowType().GetFields()[i].GetName()`, not `field.GetType()`.

So for this library, **`ResultSetMetadata` is not required**; **`StructType` (row type)
or `[]string` names** are sufficient for schema registration when formatting uses GCVs.

## Current writer API (v0.4.0)

### Schema initialization (column names)

| Entry | Input | What is stored |
|-------|--------|----------------|
| `WithMetadata(metadata)` | `*ResultSetMetadata` | `columnNames` from `row_type.fields[].name` |
| `Prepare(metadata)` | same | validates / sets `columnNames` |
| First `WriteValues(names, gcvs)` | `[]string` | sets `columnNames` if unset |
| `WriteRow(row)` | `*spanner.Row` | names from `row.ColumnNames()`, values as GCV |

There is **no** `WithColumnNames` or `WithRowType`.

### Per-row write

| Method | Requires pre-set `columnNames` | Row payload |
|--------|-------------------------------|-------------|
| `WriteGCVs(gcvs)` | yes | `[]GCV` |
| `WriteValues(names, gcvs)` | no (names on first call) | names + GCV |
| `WriteRow(row)` | no | Row extracts names + GCV |

One-shot helpers (`FormatDelimitedValues`, `FormatJSONLValues`) take `columnNames` + GCVs
per call.

## Two schema lineages (proposed contract)

Separate **what is fixed at writer setup** from **what varies per row**.

### Lineage A — Schema: column names + column types (`rowType`)

```text
Setup:    WithRowType(rowType)   // or WithMetadata(m) delegating to m.GetRowType()
Per row:  WriteValues([]*structpb.Value)
```

- Format path: `typeValueToGCV(fields[i].Type, values[i])` then `FormatColumn`.
- **Contract**: `fields[i].Type` must be non-nil; nil field type → runtime error
  (consistent with `structFieldType` / `ErrNilStructField` elsewhere).
- Column names come from `fields[i].Name` (and `UnnamedFieldNamer` where applicable).
- Does **not** require fabricated `ResultSetMetadata`.

### Lineage B — Schema: column names only; types on each row

```text
Setup:    WithColumnNames(names)   // proposed
Per row:  WriteGCVs(gcvs)
```

- Format path: `FormatColumn(gcv)` using **each GCV's `Type` and `Value`**.
- **Contract**: each `gcv.Type` must be valid; nil type → runtime error at format time.
- Row-type types in metadata are **not** consulted (even if caller also has rowType).

### Lineage C — No fixed schema (per-row full row)

```text
Per row:  WriteValues(columnNames, gcvs) or WriteRow(row)
```

- First non-empty `columnNames` wins; later rows must match (`ErrColumnNamesMismatch`).

**Do not mix lineages** on one writer without clear API separation: e.g.
`WithColumnNames` + `WriteValues([]*structpb.Value)` should be rejected because schema
has no types.

**Cross-lineage consistency**: spanvalue does not verify that schema types match GCV
types when both are present; GCV wins for formatting. Document-only expectation.

## Layered API view

```text
[L0] Cell     FormatColumn(GCV)                    types + values from GCV
[L1] Row      formatColumns([]GCV)                 no column names
[L2] Row      FormatRowColumns(names, []GCV)       names + GCV
[L3] Schema   Prepare* / With*                     register names ± types
[L4] Stream   Write*                               payload per lineage above
```

## `WithMetadata` vs `WithRowType`

**Today**: `WithMetadata` only consumes names from `RowType`.

**Should be**: same role as `WithRowType` — register row schema. Suggested evolution:

1. Add `WithRowType(*sppb.StructType)` (and optionally store fields for lineage A).
2. Implement `WithMetadata(m)` as `WithRowType(m.GetRowType())` plus any future
   metadata-only needs.
3. Add `WithColumnNames([]string)` for lineage B.

`Prepare(metadata)` should share internals with `PrepareRowType` / `prepareColumnNames`.

## Why `WithRowType` is valid

Callers with `rowType` already avoid:

- Wrapping into `ResultSetMetadata` with unused fields, or
- Manually extracting `[]string` and discarding types they already have.

`WithRowType` moves that adapter into the library and opens a path to
`WriteValues([]*structpb.Value)` without dummy types in metadata.

## Why `WithColumnNames` is valid

Callers with only GCVs and known names should not build fake metadata. Today they can
use the first `WriteValues(names, gcvs)` call, but intent is implicit. Explicit
`WithColumnNames` matches lineage B.

## Spanner GoogleSQL note (INSERT kinds)

`SQLInsertKind` / `WithSQLInsertKind` emit `INSERT`, `INSERT OR IGNORE`, and
`INSERT OR UPDATE` prefixes documented as valid [GoogleSQL DML](https://cloud.google.com/spanner/docs/reference/standard-sql/dml-syntax).
That feature is orthogonal to schema registration but shares the same writer.

## Proposed additions (priority)

1. **`WithRowType(*sppb.StructType)`** — register names; hold `[]*StructType_Field` for lineage A.
2. **Refactor `WithMetadata` / `Prepare`** — delegate to row-type helpers (`columnNamesFromRowType`).
3. **`WithColumnNames([]string)`** — lineage B setup.
4. **`WriteProtoValues([]*structpb.Value)`** (name TBD) — lineage A per-row write; error if schema types missing/nil.
5. **Godoc** on writers — document the two lineages and that metadata types are ignored when using GCVs.

Optional later: validate `len(values) == len(schema.Fields)` on lineage A; still no
metadata-vs-GCV type equality checks unless explicitly requested.

## References in tree

| Location | Role |
|----------|------|
| `writer/metadataColumnNames` | names from metadata row type |
| `writer/WriteGCVs` | requires `columnNames` already set |
| `row.go` `FormatRowColumns` | names for labeling; format from GCV |
| `common.go` `FormatColumn` | uses `value.Type` |
| `writer/writer_test.go` `metadataWithColumnNames` | dummy metadata pattern tests use |

## Related issues

- Writer constructor / export cleanup: #84
- SQL insert variants (kinds implemented; batching open): #79
- Nullable `gcvctor` input naming: #43

This design doc does not close those issues; it narrows a follow-up for writer schema APIs.
