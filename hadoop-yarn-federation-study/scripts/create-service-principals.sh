#!/usr/bin/env bash
set -euo pipefail

REALM="${REALM:-EXAMPLE.COM}"
KEYTAB_DIR="${KEYTAB_DIR:-/tmp/hadoop-keytabs}"

mkdir -p "${KEYTAB_DIR}"
rm -f "${KEYTAB_DIR}"/*.keytab

add_randkey() {
    local principal="$1"
    if kadmin.local -q "getprinc ${principal}" >/dev/null 2>&1; then
        kadmin.local -q "cpw -randkey ${principal}" >/dev/null
    else
        kadmin.local -q "addprinc -randkey ${principal}" >/dev/null
    fi
}

ktadd_file() {
    local file="$1"
    local principal="$2"
    kadmin.local -q "ktadd -k ${file} ${principal}" >/dev/null
}

add_randkey "nn/nn1.lab.example.com@${REALM}"
add_randkey "dn/prod1.lab.example.com@${REALM}"
add_randkey "dn/dr1.lab.example.com@${REALM}"
add_randkey "rm/prod1.lab.example.com@${REALM}"
add_randkey "rm/dr1.lab.example.com@${REALM}"
add_randkey "nm/prod1.lab.example.com@${REALM}"
add_randkey "nm/dr1.lab.example.com@${REALM}"
add_randkey "router/router1.lab.example.com@${REALM}"
add_randkey "jhs/nn1.lab.example.com@${REALM}"
add_randkey "HTTP/nn1.lab.example.com@${REALM}"
add_randkey "HTTP/prod1.lab.example.com@${REALM}"
add_randkey "HTTP/dr1.lab.example.com@${REALM}"
add_randkey "HTTP/router1.lab.example.com@${REALM}"
add_randkey "sparkuser@${REALM}"

ktadd_file "${KEYTAB_DIR}/nn.service.keytab" "nn/nn1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/dn-prod.service.keytab" "dn/prod1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/dn-dr.service.keytab" "dn/dr1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/rm-prod.service.keytab" "rm/prod1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/rm-dr.service.keytab" "rm/dr1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/nm-prod.service.keytab" "nm/prod1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/nm-dr.service.keytab" "nm/dr1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/router.service.keytab" "router/router1.lab.example.com@${REALM}"
ktadd_file "${KEYTAB_DIR}/jhs.service.keytab" "jhs/nn1.lab.example.com@${REALM}"

kadmin.local -q "ktadd -k ${KEYTAB_DIR}/http.service.keytab HTTP/nn1.lab.example.com@${REALM} HTTP/prod1.lab.example.com@${REALM} HTTP/dr1.lab.example.com@${REALM} HTTP/router1.lab.example.com@${REALM}" >/dev/null
kadmin.local -q "ktadd -k ${KEYTAB_DIR}/sparkuser.keytab sparkuser@${REALM}" >/dev/null

echo "Keytabs written to ${KEYTAB_DIR}"
