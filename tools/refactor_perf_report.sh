#!/usr/bin/env bash
set -euo pipefail

runs=3
output_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      runs="$2"
      shift 2
      ;;
    --output-dir)
      output_dir="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "unknown argument: $1" >&2
      echo "usage: $0 [--runs N] [--output-dir DIR] -- <command...>" >&2
      exit 1
      ;;
  esac
done

if ! [[ "$runs" =~ ^[0-9]+$ ]] || [[ "$runs" -lt 1 ]]; then
  echo "--runs must be integer >= 1" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "usage: $0 [--runs N] [--output-dir DIR] -- <command...>" >&2
  exit 1
fi

if [[ -n "$output_dir" ]]; then
  mkdir -p "$output_dir"
fi

report_json=""
if [[ -n "$output_dir" ]]; then
  report_json="$output_dir/perf_report.json"
fi

python3 - "$runs" "$report_json" "$@" <<'PY'
import json
import statistics
import subprocess
import sys
import time

runs = int(sys.argv[1])
report_json = sys.argv[2]
cmd = sys.argv[3:]

durations_ms = []
for idx in range(runs):
    start = time.perf_counter()
    completed = subprocess.run(cmd, check=False)
    end = time.perf_counter()
    if completed.returncode != 0:
        print(f"run_{idx+1}_exit_code: {completed.returncode}")
        raise SystemExit(completed.returncode)
    durations_ms.append((end - start) * 1000.0)

median_ms = statistics.median(durations_ms)
result = {
    "command": cmd,
    "runs": runs,
    "durations_ms": durations_ms,
    "median_ms": median_ms,
}

print("perf_command:", " ".join(cmd))
for i, value in enumerate(durations_ms, start=1):
    print(f"run_{i}_ms: {value:.3f}")
print(f"median_ms: {median_ms:.3f}")

if report_json:
    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print("artifact_perf_json:", report_json)
PY
