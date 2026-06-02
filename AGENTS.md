# Agent instructions for `spanvalue`

Go library: format `spanner.GenericColumnValue` / `*spanner.Row` to text; build GCVs in `gcvctor/`; stream exports in `writer/`. Target **Go 1.23** (`go.mod`; toolchain `go1.23.2`). Alias **`sppb`** = `cloud.google.com/go/spanner/apiv1/spannerpb`.

## Commands

Prefer **`make check`** (verifies formatting via `fmt-check`—does not rewrite files—plus vet, build, test, golangci-lint). Also: `make fmt` (rewrite), `make build`, `make test`, `make test-v` (CI parity), `make lint`, `go test ./gcvctor -run '^TestName$'`.

PostgreSQL TypeAnnotation integration probes live in [**spanpg**](https://github.com/apstndb/spanpg) (`integration/pgtypeannotation`), not here.

## Packages

| Path | Role |
|------|------|
| Root | `FormatConfig`, presets, `ColumnNames`, `FormatRowColumns`, identifier quoting |
| `gcvctor/` | Build `GenericColumnValue` from Go types (strict; no format) |
| `writer/` | CSV/TSV/JSONL/SQL INSERT; `WriteGCVs`, `WriteRowIterator` ([writer/README.md](writer/README.md)) |
| `internal/` | Escape/literal/iterator helpers |

## Formatting

- `FormatColumn`: `FormatComplexPlugins` first (`ARRAY`/`STRUCT` included), then built-ins; plugins return **`ErrFallthrough`** to defer.
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
- **GCV slice path:** `WriteGCVs` + `WithMetadata` / `WithFormatter` / `WithUnnamedFieldNamer`. Same namer for **out-of-band headers** via root `ColumnNames(fields, namer)`.
- **Delimited:** `NewCSVWriter(out)`, `NewDelimitedWriter(out, '\t')` = **quoted TSV** (`encoding/csv`), not legacy raw TAB; raw TAB = custom `Writer` (README).
- **SQL INSERT:** `WithSQLInsertKind`, `WithSQLDialect` (identifier quoting), `WithSQLBatchSize` (>1 multi-row `VALUES`; **`Flush`** ends partial batch). `ErrInvalidSQLInsertKindForDialect`: PostgreSQL + `INSERT OR IGNORE`/`UPDATE`. After any write error, discard writer (documented). Table and formatter are constructor-only (`TableName` / `FormatConfig` accessors on `SQLInsertWriter`).

## Adoption boundaries (do not expand spanvalue into)

- **No `database/sql` / `*sql.Rows` API** in this repo: apps own metadata pseudo-rows, scan loops, stats result sets (e.g. spannersh). Document recipes only ([#109](https://github.com/apstndb/spanvalue/issues/109), [#110](https://github.com/apstndb/spanvalue/issues/110)).
- **No string→GCV parsing** in `FormatConfig` (`gcvctor` / app). PG table cells: **spanpg**, not spanvalue.

## gcvctor & errors (short)

- `IsNull`: nil `Value` or protobuf `NullValue`. `NullOf` for typed NULL; empty `ArrayValue` = length 0, not NULL.
- Strict `ArrayValue` / `StructValueOf`: `ErrTypeMismatch`, `ErrMismatchedCounts`, `ErrNilElementType`. Format: `ErrUnknownType`, `ErrMismatchedFields`.
- `Float32Value`/`Float64Value`: `NaN`/`±Inf` as strings (Spanner wire).
- **Tests:** `t.Parallel()`, `cmp.Diff`, `protocmp.Transform()`. `gcvctor` tests: expected GCV via `typector`+`structpb`, not the helper under test. Keep attribution comments in `literal_test.go`, `spanner_cli_compatible_test.go`.

## Releases & issues

- **Per-version truth:** [GitHub Releases](https://github.com/apstndb/spanvalue/releases) only—no in-repo `CHANGELOG.md`. Retroactive release edits OK with date footnote.
- **v0.4.2:** scalar plugins (#97), `WriteRowIterator` (#98). **v0.4.3+:** SQL dialect/batch, TAB/tuple docs, SQL field deprecations—not Simple NUMERIC behavior.
- Open follow-ups: [#79](https://github.com/apstndb/spanvalue/issues/79) (INSERT fragments; batching done), [#95](https://github.com/apstndb/spanvalue/issues/95) (options return errors), [#107](https://github.com/apstndb/spanvalue/issues/107) (breaking unexport).

## Git & review

- **Parallel PR work:** check out active PR branches in dedicated worktrees beside the main clone, e.g. `spanvalue-wt-<issue-or-pr>` (`git worktree add ../spanvalue-wt-124 <branch>`). Keep the main repo on docs or integration branches; run `make check` and push from the PR worktree.
- **English only** on github.com (issues, PRs, review threads).
- **Merge:** `squash and merge`; branch updates via **merge**, not rebase+force-push to `main`.
- **During review response:** avoid **rebase** on the PR branch; squash merge cleans history.
- **Gemini on PRs:** automatic review on open; do **not** post `/gemini review` right after `gh pr create` (only after review-response pushes when needed). **Go doc comment syntax** is enforced by **`godoclint`** in `make lint`—see `.gemini/styleguide.md`; do not duplicate those findings in bot review. **Temporarily** skip GitHub Copilot PR reviews. Local diff reviews: **review-router** when asked.
- **Do not merge** unless the user asks. **Do not commit** unless asked.

## Shell

Prefer single-quoted commands; escape backticks inside double quotes.
