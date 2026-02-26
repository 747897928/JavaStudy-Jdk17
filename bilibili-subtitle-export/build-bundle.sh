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

JAR_SOURCE="${PROJECT_ROOT}/bilibili-subtitle-export/target/bilibili-subtitle-export-1.0-SNAPSHOT-jar-with-dependencies.jar"
BUNDLE_DIR="${PROJECT_ROOT}/bilibili-subtitle-export/bundle"
RUN_TEMPLATE="${PROJECT_ROOT}/bilibili-subtitle-export/bundle-run.sh"
CONFIG_TEMPLATE="${PROJECT_ROOT}/bilibili-subtitle-export/example-config.json"

if [[ ! -f "${JAR_SOURCE}" ]]; then
  echo "executable jar not found: ${JAR_SOURCE}" >&2
  exit 1
fi

mkdir -p "${BUNDLE_DIR}"
cp -f "${JAR_SOURCE}" "${BUNDLE_DIR}/bilibili-subtitle-export.jar"
cp -f "${RUN_TEMPLATE}" "${BUNDLE_DIR}/run.sh"
cp -f "${CONFIG_TEMPLATE}" "${BUNDLE_DIR}/config.json"
chmod +x "${BUNDLE_DIR}/run.sh"

echo "Bundle ready:"
echo "  ${BUNDLE_DIR}/bilibili-subtitle-export.jar"
echo "  ${BUNDLE_DIR}/run.sh"
echo "  ${BUNDLE_DIR}/config.json"
