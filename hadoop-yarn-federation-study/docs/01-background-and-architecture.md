# 01. 业务背景与架构判断

## 1. 真实业务背景

你当前的场景可以抽象成下面这句话：

`prod` 和 `dr` 各自有一套独立 YARN，Spark 任务提交到哪个集群，就由哪个集群的 ResourceManager 负责生成 `applicationId`、维护状态和提供查询接口。

所以出现下面现象是正常的：

1. 在 `prod` 提交任务后得到 `application_...`
2. 直接去 `dr` 集群执行 `yarn application -status <appId>`
3. `dr` 报找不到，或者根本不知道这个 `appId`

这是因为：

1. `appId` 不是一个跨两个独立 RM 自动共享的全局数据库主键。
2. 每个 RM 只知道自己管理过的应用。
3. 两个独立 YARN 没有 Router/StateStore 时，不会共享“应用归属关系”。

## 2. YARN Federation 在这里到底解决什么

YARN Federation 的核心不是“把两个 RM 直接合并”，而是新增一层联邦控制面：

1. `Router`
2. `Federation State Store`
3. `AMRMProxy`
4. 可选的策略管理能力

典型流程是：

1. 客户端不再直连 `prod RM` 或 `dr RM`，而是统一连 `Router`
2. Router 按策略决定把应用提交到哪个子集群
3. Router 把 `application -> home sub-cluster` 映射写入 StateStore
4. 后续用户再查这个 `appId` 时，Router 能知道该去哪个 RM 取状态

这正好能解决“统一入口提交、统一入口查状态”的问题。

## 3. 它不能解决的部分，也必须提前说清楚

这块如果你后面写博客或者面试时不讲清楚，很容易被追问。

### 3.1 Federation 不是运行中 Spark 作业的跨机房容灾

如果 `prod` 整个子集群已经是应用的 `home sub-cluster`，然后整个 `prod` 宕掉：

1. Router 最多能知道这个应用原来属于 `SC-PROD`
2. 但它不能把一个已经运行到一半的 Spark ApplicationMaster 自动迁到 `SC-DR`
3. 这类诉求通常要靠更上层的重提交流程、调度平台、工作流编排和存储同步来兜底

### 3.2 直接连 RM 提交的任务，不会自动纳入联邦统一视图

这是你们最容易踩的认知坑：

1. 如果用户继续用 `spark-submit --master yarn`，但底层连的还是 `prod RM`
2. 那它就只是普通 `prod` 集群上的作业
3. 只有“通过 Router 提交”的作业，Router 才会记录映射

### 3.3 数据面不统一，控制面统一也不够

Apache Hadoop 3.4.3 官方 Federation 文档的假设里写得很明确：

1. 设计目标偏向把一个超大规模集群拆成多个子集群做扩展
2. 文档里还写了一个很重要的前提：`not looking to federate across DC yet`
3. 同时它假设底层有 `HDFS federation or equivalently scalable DFS solutions`

对你的场景，能推导出一个很关键的判断：

1. 如果 prod 和 dr 的存储完全隔离
2. 即使控制面联邦化了
3. 应用也不一定能跨子集群顺畅拿到同一份数据

所以如果你们想把它用到真实 prod/dr，需要把下面三件事一起看：

1. 计算面：YARN Federation
2. 存储面：HDFS 统一命名空间、对象存储、或者跨机房数据复制
3. 容灾面：应用级重提、队列权重切换、健康检查和自动化切流

## 4. 从组件角度理解联邦

### 4.1 Router

Router 对外暴露统一入口：

1. 接收应用提交
2. 接收应用查询
3. 接收部分 RM Admin 请求
4. 提供统一 Web UI

对你来说，Router 是“统一入口”的关键。

### 4.2 Federation State Store

它记录两类重要状态：

1. 子集群成员信息和心跳
2. 应用归属的 `home sub-cluster`

对你来说，第二点最重要，因为它决定了一个 `appId` 后续该路由到哪一个 RM。

### 4.3 AMRMProxy

AMRMProxy 跑在 NodeManager 侧，ApplicationMaster 通过它向 RM 申请资源。它存在的意义是：

1. 屏蔽多 RM
2. 支持应用跨子集群扩展
3. 保护 RM，不让 AM 直接乱打

如果你只是想先打通“统一提交 + 统一查询”，AMRMProxy 依然需要按联邦方式配置，但博客主线可以把重点放在 Router 和 StateStore。

## 5. 一个贴近你场景的推荐策略

### 5.1 平时运行

默认让大多数任务进 `SC-PROD`：

1. `root.batch` 队列权重设成 `SC-PROD=0.9`
2. `SC-DR=0.1` 甚至 `0.0`

这样 `dr` 更像冷备或演练集群。

### 5.2 灾备演练

给 Router 配第二个队列策略，比如 `root.recovery`：

1. `SC-PROD=0.0`
2. `SC-DR=1.0`

这样你在演练或切流时，只要把提交队列改成 `root.recovery`，或者批量修改权重，就可以让新作业全部进入 `dr`。

### 5.3 真正的生产建议

如果你们想认真评估上生产，至少要补齐：

1. 每个子集群内部 RM HA
2. 多 Router
3. KDC 副本
4. SQL StateStore 或更稳的状态存储方案
5. 明确的数据同步/共享路径
6. 自动化切换剧本，而不是人工敲命令

## 6. 推荐学习/实验路线

### 路线 A：先验证核心闭环

1. 两个非 HA 子集群
2. 一个 Router
3. 一个 ZooKeeper StateStore
4. 整体 Kerberos 化
5. 统一提交、统一查状态跑通

### 路线 B：再逼近生产

1. RM HA
2. 多 Router
3. SQL StateStore
4. 更合理的队列策略
5. 监控、日志、审计

## 7. 这套知识怎么写进简历

别写成“了解 YARN Federation”。太虚。

更好的写法应该像这样：

1. 基于 Hadoop 3.4.3 搭建了双子集群 `YARN Federation` 实验环境，完成 `Router + StateStore + AMRMProxy + Kerberos` 联调。
2. 结合 `prod/dr` 场景设计了统一提交与统一查询方案，验证了 `appId -> home sub-cluster` 的联邦路由机制。
3. 输出了从 0 到 1 的部署手册、配置模板和排障文档，覆盖 `Kerberos keytab/principal`、`LinuxContainerExecutor`、`Router queue policy` 等关键环节。

## 参考资料

1. Apache Hadoop 3.4.3 YARN Federation: https://hadoop.apache.org/docs/r3.4.3/hadoop-yarn/hadoop-yarn-site/Federation.html
2. Apache Hadoop Secure Mode 3.4.1: https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SecureMode.html
