#!/usr/bin/env bash
set -u

is_not_satisfied=0

extract_version() {
  local value="$1"

  if [[ "$value" =~ ([0-9]+)\.([0-9]+)(\.([0-9]+))? ]]; then
    printf "%s.%s.%s" "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[4]:-0}"
    return 0
  fi

  return 1
}

version_gte() {
  local current="$1"
  local required="$2"
  local current_major current_minor current_patch
  local required_major required_minor required_patch

  IFS='.' read -r current_major current_minor current_patch <<< "$current"
  IFS='.' read -r required_major required_minor required_patch <<< "$required"

  current_major=$((10#$current_major))
  current_minor=$((10#$current_minor))
  current_patch=$((10#${current_patch:-0}))
  required_major=$((10#$required_major))
  required_minor=$((10#$required_minor))
  required_patch=$((10#${required_patch:-0}))

  if (( current_major != required_major )); then
    (( current_major > required_major ))
    return
  fi

  if (( current_minor != required_minor )); then
    (( current_minor > required_minor ))
    return
  fi

  (( current_patch >= required_patch ))
}

first_line() {
  local value="$1"
  printf "%s" "${value%%$'\n'*}"
}

check_dependency() {
  local label="$1"
  local binary="$2"
  local required_version="$3"
  local required_display=">= v$required_version"
  local version_output current_version

  if ! command -v "$binary" >/dev/null 2>&1; then
    echo "$label is not installed! Required version: $required_display" >&2
    is_not_satisfied=1
    return
  fi

  if ! version_output="$("$binary" --version 2>&1)"; then
    if [ "$binary" = "zig" ]; then
      version_output="$("$binary" version 2>&1)" || {
        echo "$label version check failed. Required version: $required_display" >&2
        is_not_satisfied=1
        return
      }
    else
      echo "$label version check failed. Required version: $required_display" >&2
      is_not_satisfied=1
      return
    fi
  fi

  if [ "$binary" = "zig" ] && ! extract_version "$version_output" >/dev/null; then
    version_output="$("$binary" version 2>&1)" || {
      echo "$label version check failed. Required version: $required_display" >&2
      is_not_satisfied=1
      return
    }
  fi

  if ! current_version="$(extract_version "$version_output")"; then
    echo "$label version could not be parsed from: $(first_line "$version_output"). Required version: $required_display" >&2
    is_not_satisfied=1
    return
  fi

  if ! version_gte "$current_version" "$required_version"; then
    echo "$label [v$current_version], but required $required_display" >&2
    is_not_satisfied=1
  fi
}

dependencies=(
  "brew|brew|5.1.14"
  "dotenvx|dotenvx|1.69.2"
  "cqlsh|cqlsh|6.2.0"
  "make|make|3.81.0"
  "Docker|docker|28.5.2"
  "pass|pass|1.7.4"
  "zig|zig|0.16.0"
)

for dependency in "${dependencies[@]}"; do
  IFS='|' read -r label binary required_version <<< "$dependency"
  check_dependency "$label" "$binary" "$required_version"
done

if (( is_not_satisfied )); then
  exit 1
fi

echo "validate: dependency versions are satisfied"
