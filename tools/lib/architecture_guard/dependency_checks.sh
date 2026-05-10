check_app_dependency_growth() {
  local path="$1"
  [[ "$path" == crates/app/Cargo.toml ]] || return 0
  [[ -f "$path" ]] || return 0
  baseline_file_exists "$path" || return 0

  local current baseline added
  current="$(cargo_dependency_specs_from_file "$path")"
  baseline="$(cargo_dependency_specs_from_head "$path")"
  added="$(comm -13 <(printf '%s\n' "$baseline") <(printf '%s\n' "$current") | xargs)"
  if [[ -n "$added" ]]; then
    fail "$path adds or changes direct dependency spec in copybot-app quarantine: $added"
  fi
}

check_workspace_dependency_identity_growth() {
  local path="$1"
  [[ "$path" == Cargo.toml ]] || return 0
  [[ -f "$path" ]] || return 0
  baseline_file_exists "$path" || return 0

  local current baseline added
  current="$(cargo_dependency_specs_from_file "$path")"
  baseline="$(cargo_dependency_specs_from_head "$path")"
  added="$(comm -13 <(printf '%s\n' "$baseline") <(printf '%s\n' "$current") | xargs)"
  if [[ -n "$added" ]]; then
    fail "$path adds or changes workspace dependency spec; app workspace-dependency quarantine requires explicit review: $added"
  fi
}

cargo_dependency_specs_from_file() {
  python3 - "$1" <<'PY'
import re
import sys

DEP_SECTIONS = {"dependencies", "dev-dependencies", "build-dependencies"}


def strip_comment(line):
    in_quote = None
    escaped = False
    out = []
    for char in line:
        if escaped:
            out.append(char)
            escaped = False
            continue
        if char == "\\" and in_quote:
            out.append(char)
            escaped = True
            continue
        if char in {"'", '"'}:
            if in_quote == char:
                in_quote = None
            elif in_quote is None:
                in_quote = char
            out.append(char)
            continue
        if char == "#" and in_quote is None:
            break
        out.append(char)
    return "".join(out).strip()


def clean_part(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def compact(value):
    return re.sub(r"\s+", "", value.strip())


def package_alias(value):
    match = re.search(r'package\s*=\s*["\']([^"\']+)["\']', value)
    return match.group(1) if match else None


def dep_specs(text):
    specs = set()
    in_bulk_table = False
    section_prefix = ""
    table_dep = None
    table_fields = {}

    def flush_table_dep():
        nonlocal table_dep, table_fields
        if table_dep:
            package = clean_part(table_fields.get("package", table_dep))
            field_spec = ",".join(
                f"{key}={compact(value)}" for key, value in sorted(table_fields.items())
            )
            specs.add(f"{section_prefix}{table_dep}->{package}::{{{field_spec}}}")
        table_dep = None
        table_fields = {}

    def bracket_delta(line):
        in_quote = None
        escaped = False
        delta = 0
        for char in line:
            if escaped:
                escaped = False
                continue
            if char == "\\" and in_quote:
                escaped = True
                continue
            if char in {"'", '"'}:
                if in_quote == char:
                    in_quote = None
                elif in_quote is None:
                    in_quote = char
                continue
            if in_quote is not None:
                continue
            if char in "[{":
                delta += 1
            elif char in "]}":
                delta -= 1
        return delta

    logical_lines = []
    pending = ""
    depth = 0
    for raw in text.splitlines():
        line = strip_comment(raw)
        if not line:
            continue
        pending = f"{pending} {line}".strip() if pending else line
        depth += bracket_delta(line)
        if depth > 0:
            continue
        logical_lines.append(pending)
        pending = ""
        depth = 0
    if pending:
        logical_lines.append(pending)

    for line in logical_lines:
        if line.startswith("[") and line.endswith("]"):
            flush_table_dep()
            parts = [clean_part(part) for part in line.strip("[]").split(".")]
            in_bulk_table = False
            section_prefix = ""
            for index, part in enumerate(parts):
                if part not in DEP_SECTIONS:
                    continue
                section_prefix = ".".join(parts[: index + 1])
                if section_prefix:
                    section_prefix += "."
                if index == len(parts) - 1:
                    in_bulk_table = True
                elif index + 1 < len(parts):
                    table_dep = clean_part(parts[index + 1])
                break
            continue
        if table_dep and "=" in line:
            key, value = line.split("=", 1)
            table_fields[clean_part(key)] = value.strip()
            continue
        if in_bulk_table and "=" in line:
            name, value = line.split("=", 1)
            name = clean_part(name)
            value = value.strip()
            if name:
                specs.add(
                    f"{section_prefix}{name}->{package_alias(value) or name}::{compact(value)}"
                )

    flush_table_dep()
    return sorted(specs)


with open(sys.argv[1], "r", encoding="utf-8") as fh:
    for spec in dep_specs(fh.read()):
        print(spec)
PY
}

cargo_dependency_specs_from_head() {
  local tmp
  tmp="$(mktemp)"
  baseline_file_content "$1" > "$tmp"
  cargo_dependency_specs_from_file "$tmp"
  rm -f "$tmp"
}

check_forbidden_operator_deps() {
  local path="$1"
  [[ "$path" == crates/discovery-v2/Cargo.toml || "$path" == crates/operators/Cargo.toml || "$path" == crates/live-proof/Cargo.toml || "$path" == crates/storage-core/Cargo.toml || "$path" == crates/*-ops/Cargo.toml ]] || return 0
  [[ -f "$path" ]] || return 0
  local forbidden=(
    'copybot-app'
    'copybot-ingestion'
    'copybot-shadow'
    'copybot-discovery'
    'copybot-storage'
  )
  if [[ "$path" != crates/operators/Cargo.toml ]]; then
    forbidden+=(
      'yellowstone-grpc-client'
      'yellowstone-grpc-proto'
      'tonic'
    )
  fi
  local dep
  for dep in "${forbidden[@]}"; do
    if cargo_toml_mentions_dep "$path" "$dep"; then
      fail "$path declares forbidden operator dependency: $dep"
    fi
  done
}

cargo_toml_mentions_dep() {
  local path="$1"
  local dep="$2"
  awk -v dep="$dep" '
    function esc_re(value, out, i, c) {
      out = ""
      for (i = 1; i <= length(value); i++) {
        c = substr(value, i, 1)
        if (c ~ /[][(){}.^$*+?|\\-]/) {
          out = out "\\" c
        } else {
          out = out c
        }
      }
      return out
    }
    BEGIN {
      dep_re = esc_re(dep)
    }
    /^[[:space:]]*#/ { next }
    {
      line = $0
      sub(/[[:space:]]*#.*/, "", line)
      if (line ~ "^\\[[^]]*\\." dep_re "\\]") {
        found = 1
      }
      if (line ~ "^\\[[^]]*\\.\"" dep_re "\"\\]") {
        found = 1
      }
      if (line ~ "^\\[[^]]*\\.('\''" dep_re "'\'')\\]") {
        found = 1
      }
      if (line ~ "^[[:space:]]*" dep_re "[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*\"" dep_re "\"[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*'\''" dep_re "'\''[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*" dep_re "\\.workspace[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*\"" dep_re "\"\\.workspace[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*'\''" dep_re "'\''\\.workspace[[:space:]]*=") {
        found = 1
      }
      if (line ~ "package[[:space:]]*=[[:space:]]*\"" dep_re "\"") {
        found = 1
      }
      if (line ~ "package[[:space:]]*=[[:space:]]*'\''" dep_re "'\''") {
        found = 1
      }
    }
    END { exit found ? 0 : 1 }
  ' "$path"
}
