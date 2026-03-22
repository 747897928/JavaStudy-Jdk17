# 03. Hadoop 3.4.3 联邦实验环境落地步骤

这篇的目标不是一下子做成生产，而是先把这条主线打通：

`Kerberos -> HDFS -> MR/JHS -> 两个 YARN 子集群 -> Router -> 统一提交 -> 统一查状态`

## 1. 先定实验边界

### 1.1 这篇采用的方案

1. Hadoop 版本: `3.4.3`
2. 子集群数量: `2`
3. 子集群 ID:
   1. `SC-PROD`
   2. `SC-DR`
4. 联邦状态存储: `ZooKeeperFederationStateStore`
5. 每个子集群先用 `非 HA RM`
6. 整个实验环境启用 Kerberos

### 1.2 为什么先不用 RM HA

因为你现在最想验证的是：

1. 统一提交
2. 统一查询
3. `appId -> home sub-cluster` 映射

RM HA 会显著放大配置面和排障面。官方 Federation 文档也明确说了：如果子集群没有开启 HA，可以临时把 `yarn.federation.non-ha.enabled=true`，但生产推荐 RM HA。

## 2. 下载和安装 Hadoop 3.4.3

在所有 Hadoop 节点执行：

```bash
cd /opt
sudo tar -xzf hadoop-3.4.3.tar.gz
sudo ln -sfn /opt/hadoop-3.4.3 /opt/hadoop
sudo chown -R root:root /opt/hadoop-3.4.3
```

建议在 `/etc/profile.d/hadoop.sh` 写入：

```bash
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

## 3. 准备本地目录和权限

本项目已经给了一个目录初始化脚本：

[prepare-host-dirs.sh](../scripts/prepare-host-dirs.sh)

在每个 Hadoop 节点执行：

```bash
chmod +x scripts/prepare-host-dirs.sh
sudo ./scripts/prepare-host-dirs.sh
```

这个脚本会准备：

1. `/data/hadoop/tmp`
2. `/data/hadoop/dfs/name`
3. `/data/hadoop/dfs/data`
4. `/data/hadoop/yarn/local`
5. `/data/hadoop/yarn/log`
6. `/etc/security/keytabs`

## 4. 先落公共配置

把这些模板复制到每个节点的 `etc/hadoop/`：

1. [core-site.xml](../conf-templates/core-site.xml)
2. [hdfs-site.xml](../conf-templates/hdfs-site.xml)
3. [mapred-site.xml](../conf-templates/mapred-site.xml)
4. [capacity-scheduler.xml](../conf-templates/capacity-scheduler.xml)
5. [container-executor.cfg](../conf-templates/container-executor.cfg)
6. [federation-machine-list.txt](../conf-templates/federation-machine-list.txt)

重点看几个配置点。

### 4.1 core-site.xml

这里打开了：

1. `hadoop.security.authentication=kerberos`
2. `hadoop.security.authorization=true`
3. `hadoop.security.auth_to_local`
4. `hadoop.http.authentication.type=kerberos`

### 4.2 hdfs-site.xml

这里打开了：

1. NN/DN principal 和 keytab
2. `dfs.block.access.token.enable=true`
3. `dfs.data.transfer.protection=authentication`
4. `dfs.web.authentication.kerberos.*`

### 4.3 mapred-site.xml

这里配置了：

1. `mapreduce.framework.name=yarn`
2. JobHistory Server principal/keytab
3. `mapreduce.jobhistory.webapp.spnego-*`
4. 日志聚合目录

### 4.4 container-executor.cfg

YARN 开启安全容器时，`LinuxContainerExecutor` 是容易踩坑的重点。这个文件必须和 `yarn-site.xml` 里的 group 保持一致。

## 5. 配置 HDFS

### 5.1 nn1 上格式化 NameNode

```bash
sudo -u hdfs hdfs namenode -format
```

### 5.2 启动 HDFS

在 `nn1`：

```bash
sudo -u hdfs hdfs --daemon start namenode
```

在 `prod1` 和 `dr1`：

```bash
sudo -u hdfs hdfs --daemon start datanode
```

### 5.3 初始化 HDFS 目录

先拿 `hdfs` principal：

```bash
sudo -u hdfs kinit -kt /etc/security/keytabs/nn.service.keytab nn/nn1.lab.example.com@EXAMPLE.COM
```

然后创建目录：

```bash
hdfs dfs -mkdir -p /
hdfs dfs -chmod 755 /
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod 1777 /tmp
hdfs dfs -mkdir -p /user
hdfs dfs -chmod 755 /user
hdfs dfs -mkdir -p /tmp/logs
hdfs dfs -chown yarn:hadoop /tmp/logs
hdfs dfs -chmod 1777 /tmp/logs
hdfs dfs -mkdir -p /mr-history/done
hdfs dfs -chown mapred:hadoop /mr-history/done
hdfs dfs -chmod 750 /mr-history/done
hdfs dfs -mkdir -p /mr-history/tmp
hdfs dfs -chown mapred:hadoop /mr-history/tmp
hdfs dfs -chmod 1777 /mr-history/tmp
```

这些目录权限和 Hadoop Secure Mode 官方建议是一致的。

## 6. 启动 JobHistory Server

在 `nn1`：

```bash
sudo -u mapred kinit -kt /etc/security/keytabs/jhs.service.keytab jhs/nn1.lab.example.com@EXAMPLE.COM
sudo -u mapred mapred --daemon start historyserver
```

## 7. 启动 ZooKeeper

联邦实验先用一个单节点 ZK 即可。你可以：

1. 直接在 `nn1` 上部署单节点 ZK
2. 监听 `2181`
3. 把 `hadoop.zk.address` 配成 `nn1.lab.example.com:2181`

这一步不是本文重点，但必须保证 Router 和两个 RM 都能访问它。

## 8. 配置 SC-PROD

把 [yarn-site-prod.xml](../conf-templates/yarn-site-prod.xml) 放到 `prod1:/opt/hadoop/etc/hadoop/yarn-site.xml`

这份配置里最关键的是：

1. `yarn.resourcemanager.cluster-id=SC-PROD`
2. `yarn.resourcemanager.epoch=1000`
3. `yarn.federation.enabled=true`
4. `yarn.federation.state-store.class=org.apache.hadoop.yarn.server.federation.store.impl.ZookeeperFederationStateStore`
5. `yarn.federation.non-ha.enabled=true`
6. `yarn.nodemanager.amrmproxy.enabled=true`
7. `yarn.nodemanager.amrmproxy.interceptor-class.pipeline=...FederationInterceptor`

启动：

```bash
sudo -u yarn kinit -kt /etc/security/keytabs/rm.service.keytab rm/prod1.lab.example.com@EXAMPLE.COM
sudo -u yarn yarn --daemon start resourcemanager
sudo -u yarn yarn --daemon start nodemanager
```

## 9. 配置 SC-DR

把 [yarn-site-dr.xml](../conf-templates/yarn-site-dr.xml) 放到 `dr1:/opt/hadoop/etc/hadoop/yarn-site.xml`

这份配置和 `SC-PROD` 基本一致，关键差异是：

1. `yarn.resourcemanager.cluster-id=SC-DR`
2. `yarn.resourcemanager.epoch=2000`
3. 主机名和地址改为 `dr1`

启动：

```bash
sudo -u yarn kinit -kt /etc/security/keytabs/rm.service.keytab rm/dr1.lab.example.com@EXAMPLE.COM
sudo -u yarn yarn --daemon start resourcemanager
sudo -u yarn yarn --daemon start nodemanager
```

## 10. 配置 Router

把 [yarn-site-router.xml](../conf-templates/yarn-site-router.xml) 放到 `router1:/opt/hadoop/etc/hadoop/yarn-site.xml`

关键点：

1. `yarn.router.clientrm.interceptor-class.pipeline`
2. `yarn.router.rmadmin.interceptor-class.pipeline`
3. `yarn.router.webapp.interceptor-class.pipeline`
4. `yarn.router.keytab.file`
5. `yarn.router.kerberos.principal`
6. `yarn.router.interceptor.allow-partial-result.enable=true`

我额外把 `ApplicationSubmissionContextInterceptor` 也放进了 pipeline，因为官方文档明确提到：

1. 如果 StateStore 用 ZooKeeper
2. `ApplicationSubmissionContext` 大于 `1MB`
3. 可能导致 ZK 存储失败

启动：

```bash
sudo -u yarn kinit -kt /etc/security/keytabs/router.service.keytab router/router1.lab.example.com@EXAMPLE.COM
sudo -u yarn yarn --daemon start router
```

## 11. 配置联邦客户端

把 [yarn-site-client.xml](../conf-templates/yarn-site-client.xml) 放到客户端的独立配置目录，例如：

```bash
mkdir -p /opt/hadoop-client-conf
cp core-site.xml hdfs-site.xml mapred-site.xml yarn-site-client.xml /opt/hadoop-client-conf/
mv /opt/hadoop-client-conf/yarn-site-client.xml /opt/hadoop-client-conf/yarn-site.xml
```

关键点：

1. `yarn.resourcemanager.address=router1.lab.example.com:8050`
2. `yarn.resourcemanager.admin.address=router1.lab.example.com:8052`
3. `yarn.resourcemanager.scheduler.address=localhost:8049`

最后这一条看起来奇怪，但这是 Apache 官方 Federation 文档在“Running a Sample Job”章节直接给出的 client 配置。

## 12. 初始化队列权重

本项目准备了一份 [federation-weights.xml](../conf-templates/federation-weights.xml)，包含两个队列策略：

1. `root.batch`
   1. 大多数流量进 `SC-PROD`
   2. 少量流量进 `SC-DR`
2. `root.recovery`
   1. 全部流量进 `SC-DR`

在 `router1` 上导入：

```bash
yarn routeradmin -policy --batch-save --format xml -f /opt/hadoop/etc/hadoop/federation-weights.xml
```

查看策略：

```bash
yarn routeradmin -policy -list --pageSize 20 --currentPage 1 --queues root.batch,root.recovery
```

## 13. 启动顺序建议

从 0 到 1 建议按下面顺序启动：

1. KDC
2. ZooKeeper
3. HDFS: NN -> DN
4. JobHistory Server
5. `SC-PROD`: RM -> NM
6. `SC-DR`: RM -> NM
7. Router
8. Client 冒烟

## 14. 冒烟验证

项目里有个脚本：

[smoke-test-federation.sh](../scripts/smoke-test-federation.sh)

使用方法：

```bash
export HADOOP_CONF_DIR=/opt/hadoop-client-conf
export TEST_PRINCIPAL=sparkuser@EXAMPLE.COM
export TEST_KEYTAB=/etc/security/keytabs/sparkuser.keytab
chmod +x scripts/smoke-test-federation.sh
./scripts/smoke-test-federation.sh
```

它会做这些事：

1. `kinit`
2. 调 Router 查询节点
3. 提交 `pi` 示例作业
4. 抓出 `applicationId`
5. 用 Router 再查状态

## 15. 你要验证的不是“作业能跑”，而是这三个现象

### 15.1 统一提交

客户端不直接指向 `prod RM` / `dr RM`，而是指向 Router。

### 15.2 统一查询

作业提交后，依然通过 Router 查询 `appId`。

### 15.3 子集群归属可见

你应该能从 Router 维度看到：

1. 作业在哪个 `home sub-cluster`
2. 队列策略是否按预期生效

## 16. 从实验室走向更接近生产

先把上面跑通，再做这些增强：

1. 每个子集群内部上 RM HA
2. 多 Router
3. KDC 副本
4. SQL Federation StateStore
5. ZooKeeper 安全和 ACL
6. 更严谨的队列权重切换剧本

## 17. 和你们 prod/dr 场景直接相关的判断

如果你要把这套东西带回工作里，最重要的判断不是“能不能配置起来”，而是下面这几个问题：

1. 以后 Spark 提交入口能不能统一切到 Router
2. prod/dr 的存储是不是能做到可访问或可同步
3. 你们要的是“统一查询”，还是“运行中作业容灾”
4. 如果 prod 故障，是要自动切新作业到 dr，还是要旧作业也接续执行

只有第一个和第二个解决了，联邦才有真正落地价值。

## 参考资料

1. Apache Hadoop 3.4.3 YARN Federation: https://hadoop.apache.org/docs/r3.4.3/hadoop-yarn/hadoop-yarn-site/Federation.html
2. Apache Hadoop Secure Mode 3.4.1: https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SecureMode.html
3. Apache Hadoop Single Node Setup 3.4.3: https://hadoop.apache.org/docs/r3.4.3/hadoop-project-dist/hadoop-common/SingleCluster.html
