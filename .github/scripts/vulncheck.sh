#!/usr/bin/env bash
set -euo pipefail

database="${1:?expected a govulncheck database URL}"
base="${2:-}"
scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT
export GOWORK=off LC_ALL=C

scan() {
  local directory="$1" result="$2"
  GOFLAGS=-mod=readonly govulncheck -C "$directory" -db "$database" -format=json ./... >"$result.json"
  jq -ces '
    map(.config // empty)
    | if length == 1 and .[0].scan_mode == "source" and .[0].scan_level == "symbol"
        and .[0].db_last_modified != null
      then .[0]
      else error("expected a symbol-level source scan with database metadata")
      end
  ' "$result.json" >"$result.config"
  jq -rs '
    def symbol: .module, .package, (.receiver // ""), .function;
    .[] | .finding // empty | select(.trace[0].function)
    | [.osv, (.trace[0] | symbol), (.trace[-1] | symbol)] | @tsv
  ' "$result.json" | sort -u >"$result.paths"
  cut -f1-5 "$result.paths" | sort -u >"$result.symbols"
}

show_findings() {
  jq -rs '
    .[] | .finding // empty | select(.trace[0].function)
    | [.osv, .trace[0].module, .trace[0].version, (.fixed_version // "unavailable")] | @tsv
  ' "$1.json" | sort -u
}

regressions=0
if [[ -z "$base" ]]; then
  # Main and scheduled runs alert on advisory IDs outside the tracked set.
  sed -n '/^GO-[0-9][0-9]*-[0-9][0-9]*$/p' .github/scripts/vulncheck-advisories.txt |
    sort -u >"$scratch/tracked.ids"
fi
for module in . dbsqlrows/gospanner; do
  printf '\n%s\n' "Module $module"
  scan "$module" "$scratch/head"
  printf '%s\n' 'Reported advisories (ID, affected module, selected version, fixed version)'
  show_findings "$scratch/head"

  if [[ -z "$base" ]]; then
    cut -f1 "$scratch/head.symbols" | sort -u >"$scratch/head.ids"
    comm -13 "$scratch/tracked.ids" "$scratch/head.ids" >"$scratch/untracked.ids"
    if [[ -s "$scratch/untracked.ids" ]]; then
      printf '%s\n' '::error::Found untracked advisories. Review the reported symbols and fixed versions.'
      cat "$scratch/untracked.ids"
      regressions=1
    fi
    continue
  fi

  scan "$base/$module" "$scratch/base"
  if ! cmp -s "$scratch/base.config" "$scratch/head.config"; then
    printf '%s\n' '::error::Base and head scans used different scanner, toolchain, or database configurations.'
    diff -u "$scratch/base.config" "$scratch/head.config" || true
    exit 1
  fi
  comm -23 "$scratch/base.symbols" "$scratch/head.symbols" >"$scratch/removed"
  comm -13 "$scratch/base.symbols" "$scratch/head.symbols" >"$scratch/added"
  comm -13 "$scratch/base.paths" "$scratch/head.paths" >"$scratch/paths"

  if [[ -s "$scratch/removed" ]]; then
    printf '%s\n' 'Removed vulnerable symbols (ID, module, package, receiver, function)'
    cat "$scratch/removed"
  fi
  # The scanner chooses representative traces. Review entry-point changes
  # separately because a different representative can describe an existing path.
  if [[ -s "$scratch/paths" ]]; then
    printf '%s\n' 'Newly reported representative entry points (ID, symbol, entry point)'
    cat "$scratch/paths"
  fi
  if [[ -s "$scratch/added" ]]; then
    printf '%s\n' 'New vulnerable symbols (ID, module, package, receiver, function)'
    cat "$scratch/added"
    regressions=1
  else
    printf '%s\n' 'No new vulnerable symbols.'
  fi
done

exit "$regressions"
