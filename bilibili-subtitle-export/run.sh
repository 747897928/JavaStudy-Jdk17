#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -x "/usr/libexec/java_home" ]]; then
  JAVA17_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  if [[ -n "${JAVA17_HOME}" ]]; then
    export JAVA_HOME="${JAVA17_HOME}"
    export PATH="${JAVA_HOME}/bin:${PATH}"
  fi
fi

cd "${PROJECT_ROOT}"
mvn -pl bilibili-subtitle-export -am -DskipTests package

JAR_PATH="${PROJECT_ROOT}/bilibili-subtitle-export/target/bilibili-subtitle-export-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "executable jar not found: ${JAR_PATH}" >&2
  exit 1
fi

java -jar "${JAR_PATH}" "$@"
