#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_PATH="${SCRIPT_DIR}/bilibili-subtitle-export.jar"

if [[ -x "/usr/libexec/java_home" ]]; then
  JAVA17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  if [[ -n "${JAVA17_HOME}" ]]; then
    export JAVA_HOME="${JAVA17_HOME}"
    export PATH="${JAVA_HOME}/bin:${PATH}"
  fi
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "jar not found: ${JAR_PATH}" >&2
  echo "run ./build-bundle.sh first" >&2
  exit 1
fi

java -jar "${JAR_PATH}" "$@"
