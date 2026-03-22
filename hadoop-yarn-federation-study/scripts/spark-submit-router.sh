#!/usr/bin/env bash
set -euo pipefail

: "${SPARK_HOME:?SPARK_HOME is required}"
: "${HADOOP_CONF_DIR:?HADOOP_CONF_DIR must point to the Router client config directory}"

APP_RESOURCE="${1:?Usage: spark-submit-router.sh <app-jar-or-py> [spark-submit args...]}"
shift

DEPLOY_MODE="${SPARK_DEPLOY_MODE:-cluster}"
YARN_QUEUE="${YARN_QUEUE:-root.batch}"
SPARK_PRINCIPAL="${SPARK_KRB_PRINCIPAL:-}"
SPARK_KEYTAB="${SPARK_KRB_KEYTAB:-}"

args=(
  --master yarn
  --deploy-mode "${DEPLOY_MODE}"
  --queue "${YARN_QUEUE}"
)

if [[ -n "${SPARK_PRINCIPAL}" && -n "${SPARK_KEYTAB}" ]]; then
  args+=(--principal "${SPARK_PRINCIPAL}" --keytab "${SPARK_KEYTAB}")
fi

exec "${SPARK_HOME}/bin/spark-submit" "${args[@]}" "${APP_RESOURCE}" "$@"
