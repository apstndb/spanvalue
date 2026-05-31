# Changelog

Release notes for [github.com/apstndb/spanvalue](https://github.com/apstndb/spanvalue).
Per-version details also appear on [GitHub Releases](https://github.com/apstndb/spanvalue/releases).

## v0.4.3 (unreleased)

### writer

- `WithSQLDialect` for SQL INSERT identifier quoting ([#104](https://github.com/apstndb/spanvalue/pull/104))
- `WithSQLBatchSize`, `SQLInsertWriter.Flush`, `ErrInvalidSQLInsertKindForDialect`, and table-change batch flush ([#106](https://github.com/apstndb/spanvalue/pull/106); partial [#79](https://github.com/apstndb/spanvalue/issues/79))
- **Deprecated:** `SQLInsertWriter.Table` and `SQLInsertWriter.Formatter` — set via constructor / `WithFormatter` only ([#107](https://github.com/apstndb/spanvalue/issues/107))

### docs

- Quoted TSV vs raw tab-separated output ([#102](https://github.com/apstndb/spanvalue/pull/102))
- Tuple STRUCT with Spanner CLI scalars (`Clone` + `FormatTupleStruct`) ([#105](https://github.com/apstndb/spanvalue/pull/105))

## v0.4.2

### root (`spanvalue`)

- Scalar `FormatComplexFunc` plugins on presets (`FormatSimpleValue`, `FormatLiteralValue`, `FormatSpannerCLIValue`) and `FormatConfigWithoutScalarPlugins ([#97](https://github.com/apstndb/spanvalue/pull/97))
- **NUMERIC on `SimpleFormatConfig`:** wire string as-is (e.g. `"99.5"` stays `99.5` in CSV/JSONL). Upgrading from **v0.4.1 or older** Simple export may require regolden tests; this is **not** a v0.4.3-only change.
- BYTES fast paths on Simple/Literal/CLI presets (same PR)

### writer

- `RunRowIterator` and `WriteRowIterator` for `*spanner.RowIterator` ([#98](https://github.com/apstndb/spanvalue/pull/98))

## v0.4.1

### writer

- `WithRowType`, `WriteStructValues`, and zero-column `Flush` behavior for empty result metadata

## Migration: NUMERIC on Simple export

| `FormatConfig` preset | NUMERIC output |
|-----------------------|----------------|
| `SimpleFormatConfig` (default for CSV/JSONL writers) | Wire string as-is (**since v0.4.2**) |
| `LiteralFormatConfig` | SQL literal / `NumericString` padding |
| `SpannerCLICompatibleFormatConfig` | `trimSpannerCLINumericFraction` on wire |

Consumers on **v0.4.1 or older** who use `SimpleFormatConfig` (explicitly or via writer defaults) should regolden exports when upgrading to **v0.4.2+**, even if they skip v0.4.3.
