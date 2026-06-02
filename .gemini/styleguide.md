# spanvalue — Gemini Code Assist review style guide

## For editors

Gemini Code Review **does not run tools** and **does not receive tool output** (no `make lint`, no CI logs). It only sees the PR diff and this file.

Write **assumptions** for the model (lint already ran; do not duplicate diagnostics), not lectures about what the bot cannot do. Config: `.golangci.yml` (`godoclint` on non-`*_test.go` files).

---

## Go doc comments

One **optional slice** of review—not the main job. Behavior, tests, API design, and Markdown still get normal scrutiny.

Standard godoc hygiene on production `.go` files is already checked in **CI and local `make lint`**. Those findings appear on the PR; **do not repeat them** or nit comment formatting CI would flag.

### When Go doc is worth a review note

Only if it helps readers beyond what lint already covers:

| Angle | Example |
|-------|---------|
| Link clarity on pkg.go.dev | Package doc uses `[WriteRow]` where `[Writer.WriteRow]` would link |
| Clearly wrong bracket target | `[Nonexistent.Method]` with no such symbol |
| Intentional package docs | `writer/doc.go` uses qualified links like `[Writer.WriteRow]`—do not “simplify” to `[WriteRow]` |

Frame optional notes as **reader / pkg.go.dev clarity**, not “lint failed” or “run make lint.”

### Common model mistakes (Go doc)

Avoid these false review threads:

| Mistake | Reality |
|---------|---------|
| Treating `[WriteRow]` vs `[Writer.WriteRow]` as a CI/lint issue | Lint does not check symbol link resolution; optional doc quality only. |
| Requiring godoc on every export | Not required by this repo’s lint config. |
| Requiring `[fmt.Stringer]` instead of plain `fmt.Stringer` | Optional style; not enforced in CI. |
| Demanding godoc fixes in `*_test.go` | Test files are outside godoc lint in `.golangci.yml`. |
| Applying godoc rules to `README.md` | Markdown is separate from Go comment lint. |
| Copying lint diagnostic text into the review | Already on the PR from CI. |
