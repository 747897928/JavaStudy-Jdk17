# 04. 常见错误与排障手册

这一篇按“现象 -> 原因 -> 处理”来写，方便你后面直接扩成博客的踩坑章节。

## 1. 架构认知类问题

### 1.1 `prod` 的 appId 到 `dr` 查不到

### 现象

```bash
yarn application -status application_1234567890000_0001
```

在 `prod` 能查到，在 `dr` 查不到。

### 原因

这是两个独立 RM 的正常行为，不是 bug。

### 处理

1. 统一让客户端通过 Router 提交
2. 统一让客户端通过 Router 查询
3. 不要再把“跨 RM 直接查同一个 appId”当成联邦前置能力

## 2. Router/Federation 类问题

### 2.1 Router 提交成功，但后续 Router 查询不到 appId

### 常见原因

1. 作业其实不是通过 Router 提交的
2. `yarn.federation.enabled` 没有在 Router/RM/NM 同时打开
3. `FederationStateStore` 不可用
4. `cluster-id` 配置错，导致子集群注册异常

### 处理

1. 检查客户端 `yarn.resourcemanager.address` 是否指向 Router
2. 检查 Router、RM、NM 的 `yarn-site.xml`
3. 检查 `hadoop.zk.address`
4. 查看 Router 日志里是否有 state store 写入异常

### 2.2 Router 只能看到一个子集群

### 常见原因

1. 另一个 RM 没有正常心跳到 StateStore
2. 两个子集群的 `yarn.resourcemanager.cluster-id` 配成了一样
3. 防火墙拦了 Router/ZK/RM 的通信

### 处理

1. 检查两个 cluster-id 是否唯一
2. 检查 ZK 连接
3. 检查 RM 日志
4. 必要时使用：

```bash
yarn routeradmin -deregisterSubCluster -sc SC-DR
```

重新观察子集群注册状态

### 2.3 应用提交时报 `ApplicationSubmissionContext` 太大

### 现象

大应用、超长 classpath、很多资源文件时，Router 提交失败。

### 原因

官方 Federation 文档明确提到：

1. 如果用的是 `ZookeeperFederationStateStore`
2. `ApplicationSubmissionContext` 超过 `1MB`
3. 可能导致 ZK 写失败

### 处理

1. 打开 `ApplicationSubmissionContextInterceptor`
2. 调整 `yarn.router.asc-interceptor-max-size`
3. 如果业务确实很重，考虑改成 SQL StateStore

## 3. Kerberos 类问题

### 3.1 `Clock skew too great`

### 原因

节点时钟偏差过大。

### 处理

1. 启用 `chronyd`
2. 检查所有节点时间
3. 先修时间，再看其它错误

### 3.2 `Server not found in Kerberos database`

### 常见原因

1. principal 主机名不对
2. 用了短主机名
3. DNS 或 `/etc/hosts` 不一致
4. `_HOST` 展开出来的 FQDN 和你建 principal 时不一致

### 处理

1. 全部统一用 FQDN
2. `hostname -f` 看实际主机名
3. `klist -kte keytab` 对照 principal

### 3.3 `Preauthentication failed`

### 常见原因

1. keytab 过期或旧版本
2. principal 重建后没重发 keytab
3. realm 打错

### 处理

1. 重新 `ktadd`
2. 重新分发 keytab
3. 清理缓存后重试：

```bash
kdestroy
kinit -kt /path/to/keytab principal@REALM
```

### 3.4 `Client not found in Kerberos database`

### 原因

principal 根本没建成功，或者 realm 配错了。

### 处理

```bash
sudo kadmin.local -q "getprinc rm/prod1.lab.example.com@EXAMPLE.COM"
```

## 4. Hadoop/YARN 安全模式类问题

### 4.1 ResourceManager 或 NodeManager 启动即退出

### 优先检查

1. 对应 principal 是否能 `kinit -kt`
2. `core-site.xml` 是否打开了 `kerberos`
3. `auth_to_local` 是否能正确映射到 `yarn`
4. `container-executor.cfg` 是否和 `yarn-site.xml` 一致

### 4.2 `User not found` / `No auth_to_local rule applied`

### 原因

principal 名能认证，但映射不到本地 Unix 用户。

### 处理

1. 检查 `hadoop.security.auth_to_local`
2. 用官方推荐的思路把 `nn/dn` 映射到 `hdfs`
3. 把 `rm/nm/router` 映射到 `yarn`
4. 把 `jhs` 映射到 `mapred`

### 4.3 NodeManager 报 `container-executor` 权限错误

### 原因

这是安全模式下最常见的坑之一。Hadoop Secure Mode 官方文档对权限要求非常严格：

1. `container-executor` 应该是 `root:hadoop`
2. 权限通常是 `6050`
3. `container-executor.cfg` 应该是只读
4. `yarn.nodemanager.local-dirs`、`log-dirs` 必须归 `yarn:hadoop`

### 处理

```bash
sudo chown root:hadoop /opt/hadoop/bin/container-executor
sudo chmod 6050 /opt/hadoop/bin/container-executor
sudo chown root:hadoop /opt/hadoop/etc/hadoop/container-executor.cfg
sudo chmod 400 /opt/hadoop/etc/hadoop/container-executor.cfg
sudo chown -R yarn:hadoop /data/hadoop/yarn
sudo chmod 755 /data/hadoop/yarn/local /data/hadoop/yarn/log
```

## 5. HDFS / JobHistory / 日志聚合问题

### 5.1 `yarn logs -applicationId` 查不到日志

### 常见原因

1. `yarn.log-aggregation-enable` 没开
2. `/tmp/logs` 权限不对
3. JobHistory 没启动
4. JHS principal/keytab 错

### 处理

1. 检查 `mapred-site.xml`
2. 检查 HDFS 上 `/tmp/logs`
3. 检查 JHS 进程和日志

### 5.2 JobHistory Web UI 401 或打不开

### 原因

SPNEGO 没配好，或者 `HTTP/_HOST` keytab 不匹配。

### 处理

1. 检查 `mapreduce.jobhistory.webapp.spnego-principal`
2. 检查 `mapreduce.jobhistory.webapp.spnego-keytab-file`
3. 对照 `klist -kte /etc/security/keytabs/http.service.keytab`

## 6. Spark on YARN 的额外提醒

这部分是结合你的场景做的实践性补充，不是官方 Federation 文档的直接配置项。

### 6.1 Spark 任务提交后能看到 appId，但作业中途因为凭据过期失败

### 原因

如果是长跑 Spark 任务，单纯本地 `kinit` 拿到的 TGT 不一定够整个生命周期。

### 建议

1. 评估使用 `spark-submit --principal --keytab`
2. 或对应的 `spark.yarn.principal` / `spark.yarn.keytab`
3. 保证 Spark ApplicationMaster 能续租或重新获取所需凭据

### 6.2 你只切了 Router，没有切 Spark 提交配置

### 现象

你以为在走联邦，实际上 Spark 还在直连某个 RM。

### 处理

明确检查：

1. `HADOOP_CONF_DIR`
2. `YARN_CONF_DIR`
3. `spark-submit` 所使用的 `yarn-site.xml`

## 7. 官方建议的辅助诊断手段

Hadoop Secure Mode 官方文档明确推荐：

1. `HADOOP_JAAS_DEBUG=true`
2. `sun.security.krb5.debug=true`
3. `hadoop kdiag`

示例：

```bash
export HADOOP_JAAS_DEBUG=true
export HADOOP_OPTS="-Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true"
hadoop kdiag
```

## 8. 我建议你写博客时保留的“真实踩坑列表”

1. FQDN 和 principal 不一致
2. `appId` 直接跨 RM 查不到
3. Router pipeline 漏配
4. `container-executor` 权限错误
5. HDFS 日志聚合目录权限错误
6. 客户端实际上没走 Router

## 参考资料

1. Apache Hadoop Secure Mode 3.4.1: https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SecureMode.html
2. Apache Hadoop 3.4.3 YARN Federation: https://hadoop.apache.org/docs/r3.4.3/hadoop-yarn/hadoop-yarn-site/Federation.html
