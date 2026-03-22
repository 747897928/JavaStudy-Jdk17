#!/usr/bin/env bash
set -euo pipefail

groupadd -f hadoop
id hdfs >/dev/null 2>&1 || useradd -g hadoop hdfs
id yarn >/dev/null 2>&1 || useradd -g hadoop yarn
id mapred >/dev/null 2>&1 || useradd -g hadoop mapred

mkdir -p /data/hadoop/tmp
mkdir -p /data/hadoop/dfs/name
mkdir -p /data/hadoop/dfs/data
mkdir -p /data/hadoop/yarn/local
mkdir -p /data/hadoop/yarn/log
mkdir -p /etc/security/keytabs

chown -R hdfs:hadoop /data/hadoop/dfs
chown -R yarn:hadoop /data/hadoop/yarn
chmod 700 /data/hadoop/dfs/name /data/hadoop/dfs/data
chmod 755 /data/hadoop/yarn/local /data/hadoop/yarn/log
chmod 755 /data/hadoop/tmp
chmod 700 /etc/security/keytabs

echo "Host directories prepared."
