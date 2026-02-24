#!/usr/bin/env bash

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

normalize_bool_token() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    1|true|yes|on)
      printf 'true'
      ;;
    *)
      printf 'false'
      ;;
  esac
}

extract_field() {
  local key="$1"
  local text="$2"
  printf '%s\n' "$text" | awk -F': ' -v key="$key" '
    $1 == key {
      print substr($0, index($0, ": ") + 2)
      exit
    }
  '
}

first_non_empty() {
  local value
  for value in "$@"; do
    if [[ -n "${value:-}" ]]; then
      printf '%s' "$value"
      return
    fi
  done
  printf ''
}

normalize_gate_verdict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$raw" in
    PASS|WARN|NO_DATA|SKIP)
      printf '%s' "$raw"
      ;;
    *)
      printf 'UNKNOWN'
      ;;
  esac
}

normalize_preflight_verdict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$raw" in
    PASS|SKIP|FAIL)
      printf '%s' "$raw"
      ;;
    *)
      printf 'UNKNOWN'
      ;;
  esac
}

normalize_go_nogo_verdict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$raw" in
    GO|HOLD|NO_GO)
      printf '%s' "$raw"
      ;;
    *)
      printf 'UNKNOWN'
      ;;
  esac
}

normalize_rotation_verdict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$raw" in
    PASS|WARN|FAIL)
      printf '%s' "$raw"
      ;;
    *)
      printf 'UNKNOWN'
      ;;
  esac
}

normalize_rehearsal_verdict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$raw" in
    GO|HOLD|NO_GO)
      printf '%s' "$raw"
      ;;
    *)
      printf 'UNKNOWN'
      ;;
  esac
}

sha256_file_value() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$path" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
hasher = hashlib.sha256()
with path.open("rb") as fh:
    for chunk in iter(lambda: fh.read(1024 * 1024), b""):
        hasher.update(chunk)
print(hasher.hexdigest())
PY
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$path" | awk '{print $NF}'
    return
  fi
  printf "unavailable"
}
