#!/usr/bin/env bash

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

parse_bool_token_strict() {
  local raw
  raw="$(trim_string "$1")"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    1|true|yes|on)
      printf 'true'
      ;;
    0|false|no|off)
      printf 'false'
      ;;
    *)
      return 1
      ;;
  esac
}

parse_u64_token_strict() {
  local raw
  raw="$(trim_string "$1")"
  if [[ ! "$raw" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  printf '%s' "$raw"
}

parse_timeout_sec_strict() {
  local raw min_sec max_sec parsed
  raw="$1"
  min_sec="${2:-1}"
  max_sec="${3:-86400}"
  if ! parsed="$(parse_u64_token_strict "$raw")"; then
    return 1
  fi
  if [[ "$parsed" -lt "$min_sec" || "$parsed" -gt "$max_sec" ]]; then
    return 1
  fi
  printf '%s' "$parsed"
}

resolve_timeout_command() {
  if command -v timeout >/dev/null 2>&1; then
    command -v timeout
    return 0
  fi
  if command -v gtimeout >/dev/null 2>&1; then
    command -v gtimeout
    return 0
  fi
  return 1
}

run_with_timeout_if_available() {
  local timeout_sec="$1"
  shift
  local timeout_command=""
  if timeout_command="$(resolve_timeout_command)"; then
    "$timeout_command" "$timeout_sec" "$@"
    return
  fi
  "$@"
}

normalize_bool_token() {
  local raw
  raw="$(trim_string "$1")"
  if [[ -z "$raw" ]]; then
    printf 'false'
    return 0
  fi
  if parse_bool_token_strict "$raw"; then
    return 0
  fi
  echo "invalid boolean token (expected true/false/1/0/yes/no/on/off), got: $1" >&2
  return 1
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

extract_bool_field_strict() {
  local key="$1"
  local text="$2"
  local raw=""
  raw="$(trim_string "$(extract_field "$key" "$text")")"
  if [[ -z "$raw" ]]; then
    return 1
  fi
  parse_bool_token_strict "$raw"
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
