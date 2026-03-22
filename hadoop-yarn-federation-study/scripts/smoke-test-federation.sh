#!/usr/bin/env bash
set -euo pipefail

ROUTER_HOST="${ROUTER_HOST:-router1.lab.example.com}"
TEST_PRINCIPAL="${TEST_PRINCIPAL:-sparkuser@EXAMPLE.COM}"
TEST_KEYTAB="${TEST_KEYTAB:-/etc/security/keytabs/sparkuser.keytab}"
QUEUE="${QUEUE:-root.batch}"
EXAMPLE_JAR="${EXAMPLE_JAR:-${HADOOP_HOME:-/opt/hadoop}/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.3.jar}"

if [[ ! -f "${EXAMPLE_JAR}" ]]; then
    echo "Example jar not found: ${EXAMPLE_JAR}" >&2
    exit 1
fi

echo "Kerberos login with ${TEST_PRINCIPAL}"
kinit -kt "${TEST_KEYTAB}" "${TEST_PRINCIPAL}"
klist

echo "Listing nodes through current client config"
yarn node -list

echo "Submitting test job to queue ${QUEUE}"
submit_output="$(
    yarn jar "${EXAMPLE_JAR}" -Dmapreduce.job.queuename="${QUEUE}" pi 4 1000 2>&1 | tee /dev/stderr
)"

app_id="$(printf '%s\n' "${submit_output}" | grep -o 'application_[0-9_]*' | tail -n 1)"

if [[ -z "${app_id}" ]]; then
    echo "Failed to detect application id from output." >&2
    exit 1
fi

echo "Detected application id: ${app_id}"
echo "Querying application status through Router"
yarn application -status "${app_id}"

echo "Router Web UI should be available at http://${ROUTER_HOST}:8089"
