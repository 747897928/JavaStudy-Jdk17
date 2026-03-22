# 08. RM HA、多 Router、SQL StateStore 的生产化改造路径

这一篇不是实验步骤，而是把实验环境往真实 `prod/dr` 架构推进时，应该怎么拆阶段。

## 1. 先定目标，不然容易越做越乱

对你们场景，更现实的目标应该是：

1. 统一提交入口
2. 统一应用状态查询入口
3. 新任务可以按策略分发到 `prod/dr`
4. 单个 RM 或单个 Router 故障，不要让整条入口失效

不应该一上来就承诺的目标是：

1. 运行中的 Spark 任务跨机房无缝续跑
2. 跨 DC 完全透明的联邦化计算

Apache 官方 Federation 文档本身就明确说过，设计前提并不是“跨数据中心联邦优先”。

## 2. 我建议的 4 个阶段

### 阶段 0：现状

你们现在大概率是这样：

1. `prod` 独立 RM
2. `dr` 独立 RM
3. Spark 提交侧自己选 RM 地址
4. 状态查询也各查各的

### 阶段 1：统一入口，但先不做 HA

先做：

1. 1 个 Router
2. 2 个子集群
3. 联邦 StateStore
4. Spark 统一走 Router

这一步的目标只是证明：

1. 入口能统一
2. 查询能统一
3. 队列策略能控制去向

### 阶段 2：每个子集群内部做 RM HA

这一步才开始让 `SC-PROD`、`SC-DR` 各自具备更像生产的能力。

Apache YARN RM HA 官方文档里明确要求的关键点包括：

1. `yarn.resourcemanager.ha.enabled=true`
2. `yarn.resourcemanager.ha.rm-ids`
3. 每个 RM 的 hostname/address
4. 自动故障切换和状态恢复

你的理解可以是：

1. 联邦解决“多个子集群之间的统一入口”
2. RM HA 解决“单个子集群内部的 RM 主备”

这两层不是替代关系。

### 阶段 3：多 Router + 负载均衡

官方 Federation 文档把 Router 设计成“soft-state”组件，应用到 sub-cluster 的映射可以从 StateStore 恢复出来。

这意味着：

1. 可以部署多个 Router
2. 前面挂一个 LB/VIP
3. 所有客户端只认统一域名

官方文档还建议对 Router 场景考虑缓存和会话粘性。对生产更稳的做法是：

1. `router1.company.com`
2. `router2.company.com`
3. `yarn-router.company.com` 作为 LB/VIP

### 阶段 4：把联邦 StateStore 从单节点 ZK 升级到 SQL

实验环境里单节点 ZooKeeper 够用，但生产不建议停在这里。

官方 Federation 文档明确给了 SQL StateStore 的配置方式：

1. `yarn.federation.state-store.class=...SQLFederationStateStore`
2. `yarn.federation.state-store.sql.url`
3. `yarn.federation.state-store.sql.jdbc-class`
4. `yarn.federation.state-store.sql.username`
5. `yarn.federation.state-store.sql.password`

而且官方还给了建表脚本路径：

```text
$HADOOP_HOME/sbin/FederationStateStore/MySQL/*
```

## 3. 生产推荐拓扑

结合你们 `prod/dr` 场景，我建议目标架构长这样：

```text
                +------------------------+
                |  yarn-router.company   |
                |  LB / VIP              |
                +-----------+------------+
                            |
                +-----------+------------+
                |                        |
         +------+-----+           +------+-----+
         | Router 1   |           | Router 2   |
         +------+-----+           +------+-----+
                |                        |
                +-----------+------------+
                            |
                 SQL Federation StateStore
                            |
          +-----------------+-----------------+
          |                                   |
   SC-PROD (RM HA)                     SC-DR (RM HA)
   prod-rm1 / prod-rm2                 dr-rm1 / dr-rm2
```

## 4. RM HA 这一层怎么配

项目里补了两个 HA 模板：

1. [yarn-site-prod-ha.xml](../conf-templates/yarn-site-prod-ha.xml)
2. [yarn-site-dr-ha.xml](../conf-templates/yarn-site-dr-ha.xml)

这些模板体现的是“方向”，不是直接照抄值。

你需要按自己的真实主机名改：

1. `rm-ids`
2. hostname
3. `yarn.resourcemanager.zk-address`
4. principal
5. keytab 路径

## 5. 多 Router 这一层怎么配

项目里补了一个 Router + SQL StateStore 示例：

[yarn-site-router-sql.xml](../conf-templates/yarn-site-router-sql.xml)

真实生产建议：

1. 至少 2 个 Router
2. 前面挂 LB
3. LB 对 `8050`、`8052`、Web 端口做健康检查
4. Router 保持无状态化思路，状态交给 StateStore

## 6. 为什么我建议 SQL StateStore

不是说 ZooKeeper 不能用，而是对于你这个“要往生产靠”的目标，SQL 更容易做这些事：

1. 备份
2. 容量管理
3. 审计
4. 多 Router/多子集群长期运行下的状态管理

同时，官方文档还提到 ZooKeeper StateStore 在 `ApplicationSubmissionContext` 过大时有体积限制问题，这对复杂 Spark 作业不是个好信号。

## 7. 生产化时别漏掉 RM 自己的状态恢复

联邦 StateStore 和 RM HA 的状态恢复不是一回事。

你至少要区分：

1. 联邦 StateStore
   记录 sub-cluster 信息、应用归属映射
2. RMStateStore
   记录单个子集群内 RM 的恢复信息

所以在 RM HA 场景里，还要把 `ZKRMStateStore` 或等价恢复方案配好。

## 8. 安全层面还要补的三件事

### 8.1 KDC 副本

生产不要只有一个 KDC。

### 8.2 ZooKeeper 安全

如果 RM HA 和联邦都依赖 ZK，就要补：

1. ACL
2. Kerberos
3. 隔离网络

### 8.3 密码和 keytab 托管

实验环境里可以把账号放配置里，生产不行。

生产要接：

1. 密钥管理系统
2. 运维凭据平台
3. 受控分发和轮换

## 9. 我建议的改造顺序

最稳的顺序是：

1. 先统一 Spark 提交入口到 Router
2. 再给每个 sub-cluster 做 RM HA
3. 再把单 Router 升级成多 Router + LB
4. 最后再把联邦 StateStore 升级到 SQL

原因很简单：

1. 入口统一最先产生价值
2. RM HA 解决的是子集群单点
3. 多 Router 解决的是联邦入口单点
4. SQL StateStore 解决的是长期运行稳定性和运维性

## 10. 真正落生产前的检查清单

1. Spark 客户端是否全部只认 Router 域名
2. `prod/dr` 的队列权重是否可控
3. 单个 RM 故障时，子集群内部是否可恢复
4. 单个 Router 故障时，入口是否还能用
5. StateStore 故障时，影响面是否清楚
6. HDFS/对象存储/数据同步路径是否能支撑跨子集群调度
7. Kerberos principal/keytab 轮换是否有 runbook

## 11. 最后的现实判断

如果你们当前业务诉求只是：

1. 统一提交
2. 统一查询
3. 演练时让新任务优先进 `dr`

那联邦是值得推进的。

如果你们诉求是：

1. `prod` 机房突然挂掉
2. 正在跑的 Spark 作业无感迁移到 `dr`

那就不能把希望只放在 YARN Federation 上。

## 参考资料

1. Apache Hadoop YARN Federation: https://hadoop.apache.org/docs/r3.4.3/hadoop-yarn/hadoop-yarn-site/Federation.html
2. Apache Hadoop ResourceManager HA: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerHA.html
