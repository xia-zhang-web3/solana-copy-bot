#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: tools/refactor_loc_report.sh <file> [file...]

Reports:
  - raw_loc
  - runtime_loc_excluding_cfg_test (Rust only)
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
          in_test_module = 0
          test_depth = 0
        }

        {
          line = $0

          if (in_test_module == 1) {
            test_depth += brace_delta(line)
            if (test_depth <= 0) {
              in_test_module = 0
              test_depth = 0
            }
            next
          }

          if (pending_cfg_test == 1) {
            if (line ~ /^[[:space:]]*(pub[[:space:]]+)?mod[[:space:]]+[A-Za-z0-9_]+[[:space:]]*\{/) {
              pending_cfg_test = 0
              test_depth = brace_delta(line)
              if (test_depth > 0) {
                in_test_module = 1
              } else {
                in_test_module = 0
                test_depth = 0
              }
              next
            }
            if (line ~ /^[[:space:]]*#\[/ || line ~ /^[[:space:]]*$/ || line ~ /^[[:space:]]*\/\//) {
              next
            }
            pending_cfg_test = 0
          }

          if (line ~ /^[[:space:]]*#\[cfg\(test\)\]/) {
            pending_cfg_test = 1
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
