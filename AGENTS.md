# Agent instructions for `spanvalue`

Go library: format `spanner.GenericColumnValue` / `*spanner.Row` to text; build GCVs in `gcvctor/`; stream exports in `writer/`. Target **Go 1.24+** (`go.mod`). Alias **`sppb`** = `cloud.google.com/go/spanner/apiv1/spannerpb`.

## Commands

Prefer **`make check`** (verifies formatting via `fmt-check`—does not rewrite files—plus vet, build, test, golangci-lint). Also: `make fmt` (rewrite), `make build`, `make test`, `make test-v` (CI parity), `make lint`, `go test ./gcvctor -run '^TestName$'`.

PostgreSQL TypeAnnotation integration probes live in [**spanpg**](https://github.com/apstndb/spanpg) (`integration/pgtypeannotation`), not here.

## Packages

| Path | Role |
|------|------|
| Root | `FormatConfig`, presets, `ColumnNames`, `FormatRowColumns`, identifier quoting |
| `gcvctor/` | Build `GenericColumnValue` from Go types (strict; no format) |
| `writer/` | CSV/TSV/JSONL/SQL INSERT; `WriteGCVs`, `WriteRowIterator` ([writer/README.md](writer/README.md)) |
| `dbsqlrows/` | **Experimental.** `*sql.Rows` loop; `SQLRowsHooks` / `WriteRows` → `writer.WriteGCVs`; driver-agnostic (package godoc) |
| `dbsqlrows/gospanner/` | Optional nested module: one-shot `QueryExport` + `DefaultExecOptions` (reference integration; REPLs use core `dbsqlrows` + app `ExecOptions`) |
| `internal/` | Escape/literal/iterator helpers |

## Formatting

- `FormatColumn`: `FormatComplexPlugins` first (`ARRAY`/`STRUCT` included), then built-ins; plugins return **`ErrFallthrough`** to defer. NULL is deliberately NOT pre-filtered (common.go comment: plugins may own type-specific NULL renderings); guard combinators `PluginForType` / `PluginForTypeCode` / `PluginSkippingNull` (#250) lift the boilerplate — protofmt dogfoods them. `PluginFromNullable` lifts `FormatNullableFunc` into the chain (Decode dispatch incl. PG wrappers; unknown codes fall through, other decode errors are real); + `NullableFormatterFor[T]` = per-scalar-type override with the chain as the base (no preset FormatNullable export needed).
- **Combinators (#253 additive phase):** `PluginForArray(join)` / `PluginForStruct(field, paren)` replicate the built-in non-NULL ARRAY/STRUCT branches in the chain (same error classes; NULL defers to built-in `GetNullString` — typed NULL arrays like `CAST(NULL AS …)` need a plain `PluginForTypeCode(ARRAY, …)` plugin). `PluginForStruct`'s field callback takes `Formatter` (the v0.8 `FormatStructFieldFunc` signature), not `*FormatConfig`.
- **Builder:** `NewFormatConfig(WithNullString, WithPlugin…, WithArrayFormat, WithStructFormat, WithScalarFormatter)` assembles NullString + plugin chain only (overrides most-recent-first, then array/struct, `PluginFromNullable` tail); scalar+array+struct each **required** (`Err*Required` at build), `WithPlugin` alone never satisfies them. Deprecated fields stay nil; `Validate` is relaxed: nil `FormatArray`/`FormatStruct`/`FormatNullable` OK when `FormatComplexPlugins` non-empty (runtime errors/panics if nothing claims the shape). Unknown scalar codes on builder configs error `ErrFormatNullableRequired`, not `ErrUnknownType`. Dogfooding: `format_config_dogfood_test.go` pins byte-identical preset rebuilds. **Deprecated since #253 additive phase** (removal next breaking release): `FormatConfig.FormatArray`/`FormatStruct`/`FormatNullable`/`Literal`.
- Presets (each returns fresh `*FormatConfig`): `LiteralFormatConfig`, `SimpleFormatConfig`, `SpannerCLICompatibleFormatConfig`, `JSONFormatConfig`. Preset-backed convenience wrappers: `FormatRowLiteral`, `FormatColumnLiteral`, `FormatRowSpannerCLICompatible`, `FormatColumnSpannerCLICompatible`. `FormatRowJSONObject` takes an explicit JSON-emitting `*FormatConfig` (typically from `JSONFormatConfig()`), not a preset wrapper.
- **v0.4.2+ scalar plugins** on presets: `FormatSimpleValue`, `FormatLiteralValue`, `FormatSpannerCLIValue`. Strip via `FormatConfigWithoutScalarPlugins` or edit `FormatComplexPlugins` on a **clone** (singleton configs used by convenience funcs are shared—do not mutate).
- **NUMERIC output** (wire `"99.5"` example):

  | Preset | Behavior |
  |--------|----------|
  | `SimpleFormatConfig` (CSV/JSONL default) | Wire string as-is (**since v0.4.2** / #97) |
  | `LiteralFormatConfig` | SQL literal / `NumericString` padding |
  | `SpannerCLICompatibleFormatConfig` | `trimSpannerCLINumericFraction` on wire |

  Regolden downstream tests if upgrading from **v0.4.1** Simple export—not a v0.4.3-only change.

- **Tuple STRUCT + CLI scalars:** no new preset constructor; `SpannerCLICompatibleFormatConfig().Clone()` then `FormatStruct.FormatStructParen = FormatTupleStruct` (README/example). Official spanner-cli uses bracket STRUCT `[[…]]`.
- **JSON rows:** `UnnamedFieldNamer` / `IndexedUnnamedFieldNamer`—non-empty unique names required (`nil` = empty JSON keys).

## Writer (`writer/`)

- **Native client:** `WriteRow` / `WriteRowIterator` / `RunRowIterator` for `*spanner.RowIterator`. `WriteRowIterator`/`RunRowIterator` with `RowIteratorHooksFromWriter` register metadata and call **`Flush`** in hooks. Manual `RowIterator.Next` loops need first-`Next` metadata and zero-row **`PrepareRowType` + `Flush`** (not `defer Flush`—return `Flush()` error). Do not pass `iter.Metadata` at construction when still nil.
- **In-memory / virtual rows:** `WriteRowSeq` / `RunRowSeq` (explicit `*sppb.ResultSetMetadata` + `iter.Seq2[*spanner.Row, error]`; `RowSeq(rows...)` adapts pre-built rows) reuse the internal `rowIteratorFacade` loop, so the hook contract (PrepareMetadata-once incl. zero rows, abort-without-Finish, `RowsRead`) matches `RunRowIterator`; `Stats` stays zero. Yielded error aborts; paired row ignored. `RunRowSeqDeferredMetadata` takes a metadata **func** evaluated after the first pull (runRowIterator already resolves metadata lazily) — for merged concurrent sources that publish the row type before their first yield (surveyed from spanner-mycli partitioned-query fan-in, apstndb/spanner-mycli#666).
- **GCV slice path:** `WriteGCVs` + `WithMetadata` / `WithFormatter` / `WithUnnamedFieldNamer`. Same namer for **out-of-band headers** via root `ColumnNames(fields, namer)`.
- **Delimited:** `NewCSVWriter(out)`, `NewDelimitedWriter(out, '\t')` = **quoted TSV** (`encoding/csv`), not legacy raw TAB; raw TAB = custom `Writer` (README).
- **SQL INSERT:** `WithSQLInsertKind`, `WithSQLDialect` (identifier quoting), `WithSQLBatchSize` (>1 multi-row `VALUES`; **`Flush`** ends partial batch). `ErrInvalidSQLInsertKindForDialect`: PostgreSQL + `INSERT OR IGNORE`/`UPDATE`. After any write error, discard writer (documented). Table and formatter are constructor-only (`TableName` / `FormatConfig` accessors on `SQLInsertWriter`).

## Adoption boundaries (do not expand spanvalue into)

- **`dbsqlrows/`** owns the shared `*sql.Rows` loop (`RunRows`/`RunRowsAtData` + `SQLRowsHooks`, parallel to `writer.RunRowIterator`); csv/jsonl use `SQLRowsHooksFromGCVWriter`. No go-sql-spanner in root `go.mod`. **`dbsqlrows/gospanner/`** is optional one-shot export + ExecOptions reference (not for REPLs — spannersh uses core only). Table layout and batch orchestration stay in apps. See [#109](https://github.com/apstndb/spanvalue/issues/109) / [#110](https://github.com/apstndb/spanvalue/issues/110).
- **No string→GCV parsing** in `FormatConfig` (`gcvctor` / app). PG table cells: **spanpg**, not spanvalue.
- **Reflection / client-tag Go value → GCV** (struct tags, `Null*` wrappers, `spanner.Encoder`, typed-NULL inference mirroring the official client's `encodeValue`) lives in [**spanenc**](https://github.com/apstndb/spanenc) with [**structfields**](https://github.com/apstndb/structfields) (separate Apache-2.0 module hosting the upstream `cloud.google.com/go/internal/fields` fork and `spannertag` port so MIT modules never embed upstream-derived code); `gcvctor` stays explicit, strict constructors with caller-supplied types.

## gcvctor & errors (short)

- `IsNull`: nil `Value` or protobuf `NullValue`. `NullOf` for typed NULL; empty `ArrayValue` = length 0, not NULL.
- Strict `ArrayValue` / `StructValueOf`: `ErrTypeMismatch`, `ErrMismatchedCounts`, `ErrNilElementType`. Format: `ErrUnknownType` (unsupported type code; coverage problem), `ErrMalformedWire` (known type, invalid wire payload or unexpected NULL at the wire validator; data problem—the two do not `errors.Is`-match each other), `ErrMismatchedFields`.
- `Float32Value`/`Float64Value`: `NaN`/`±Inf` as strings (Spanner wire).
- **Tests:** `t.Parallel()`, `cmp.Diff`, `protocmp.Transform()`. `gcvctor` tests: expected GCV via `typector`+`structpb`, not the helper under test. Keep attribution comments in `literal_test.go`, `spanner_cli_compatible_test.go`.

## Releases & issues

- **Per-version truth:** [GitHub Releases](https://github.com/apstndb/spanvalue/releases) only—no in-repo `CHANGELOG.md`. Retroactive release edits OK with date footnote.
- **v0.4.2:** scalar plugins (#97), `WriteRowIterator` (#98). **v0.4.3-alpha.1** (never stable-tagged): SQL dialect/batch, TAB/tuple docs—not Simple NUMERIC behavior ([v0.4.2](https://github.com/apstndb/spanvalue/releases/tag/v0.4.2)).
- **v0.5.0** ([release](https://github.com/apstndb/spanvalue/releases/tag/v0.5.0)): writer constructor `error`, `RowIteratorHooks` extensibility, field unexport (SQL INSERT [#107](https://github.com/apstndb/spanvalue/issues/107), delimited/JSONL [#140](https://github.com/apstndb/spanvalue/issues/140)), docs/lint. Upgrading: release **Upgrading to v0.5.0** section.
- Open follow-ups: [#79](https://github.com/apstndb/spanvalue/issues/79) (INSERT fragment helpers only), [#126](https://github.com/apstndb/spanvalue/issues/126) (PostgreSQL value literals for SQL INSERT), [#43](https://github.com/apstndb/spanvalue/issues/43) (gcvctor naming memo), [#24](https://github.com/apstndb/spanvalue/issues/24) (umbrella).

## Git & review

- **Parallel PR work:** check out active PR branches in dedicated worktrees beside the main clone, e.g. `spanvalue-wt-<issue-or-pr>` (`git worktree add ../spanvalue-wt-124 <branch>`). Keep the main repo on docs or integration branches; run `make check` and push from the PR worktree.
- **English only** on github.com (issues, PRs, review threads).
- **Merge:** `squash and merge`; branch updates via **merge**, not rebase+force-push to `main`.
- **During review response:** avoid **rebase** on the PR branch; squash merge cleans history.
- **Gemini on PRs:** automatic review on open; do **not** post `/gemini review` right after `gh pr create` (only after review-response pushes when needed). **Go doc comment syntax** is enforced by **`godoclint`** in `make lint`—see `.gemini/styleguide.md`; do not duplicate those findings in bot review. **Temporarily** skip GitHub Copilot PR reviews. Local diff reviews: **review-router** when asked.
- **Do not merge** unless the user asks. **Do not commit** unless asked.

## Shell

Prefer single-quoted commands; escape backticks inside double quotes.
