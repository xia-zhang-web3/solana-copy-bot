#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: tools/refactor_loc_report.sh <file> [file...]

Reports:
  - raw_loc
  - runtime_loc_excluding_cfg_test (Rust only, excludes items annotated with #[cfg(test)])
  - cfg_test_loc
EOF
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 1
fi

for file in "$@"; do
  if [[ ! -f "$file" ]]; then
    echo "error: file not found: $file" >&2
    exit 1
  fi

  raw_loc="$(wc -l < "$file" | tr -d ' ')"
  runtime_loc="$raw_loc"

  if [[ "$file" == *.rs ]]; then
    runtime_loc="$(
      awk '
        function brace_delta(line, opens, closes, tmp) {
          tmp = line
          opens = gsub(/\{/, "", tmp)
          tmp = line
          closes = gsub(/\}/, "", tmp)
          return opens - closes
        }

        BEGIN {
          runtime = 0
          pending_cfg_test = 0
          in_cfg_test_item = 0
          cfg_item_depth = 0
          cfg_item_saw_brace = 0
        }

        function process_cfg_item_line(line, delta) {
          if (line ~ /\{/) {
            cfg_item_saw_brace = 1
          }
          delta = brace_delta(line)
          cfg_item_depth += delta

          if (cfg_item_saw_brace == 1) {
            if (cfg_item_depth <= 0) {
              in_cfg_test_item = 0
              cfg_item_depth = 0
              cfg_item_saw_brace = 0
            }
          } else if (line ~ /;[[:space:]]*$/) {
            in_cfg_test_item = 0
            cfg_item_depth = 0
            cfg_item_saw_brace = 0
          }
        }

        {
          line = $0

          if (in_cfg_test_item == 1) {
            process_cfg_item_line(line)
            next
          }

          if (pending_cfg_test == 1) {
            if (line ~ /^[[:space:]]*#\[/ || line ~ /^[[:space:]]*$/ || line ~ /^[[:space:]]*\/\// || line ~ /^[[:space:]]*\/\*/) {
              next
            }
            pending_cfg_test = 0
            in_cfg_test_item = 1
            cfg_item_depth = 0
            cfg_item_saw_brace = 0
            process_cfg_item_line(line)
            next
          }

          if (line ~ /#\[cfg\(test\)\]/) {
            pending_cfg_test = 1
            inline_item = line
            sub(/^.*#\[cfg\(test\)\][[:space:]]*/, "", inline_item)
            if (inline_item !~ /^[[:space:]]*$/ &&
                inline_item !~ /^[[:space:]]*\/\// &&
                inline_item !~ /^[[:space:]]*\/\*/) {
              # Keep parity with "attribute line is runtime metadata" expectation
              # for inline test modules like `#[cfg(test)] mod tests { ... }`.
              if (inline_item ~ /^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?mod[[:space:]]+[A-Za-z0-9_]+[[:space:]]*\{/) {
                runtime += 1
              }
              pending_cfg_test = 0
              in_cfg_test_item = 1
              cfg_item_depth = 0
              cfg_item_saw_brace = 0
              process_cfg_item_line(inline_item)
            }
            next
          }

          runtime += 1
        }

        END {
          print runtime
        }
      ' "$file"
    )"
  fi

  cfg_test_loc="$((raw_loc - runtime_loc))"
  echo "file: $file"
  echo "raw_loc: $raw_loc"
  echo "runtime_loc_excluding_cfg_test: $runtime_loc"
  echo "cfg_test_loc: $cfg_test_loc"
  echo
done
