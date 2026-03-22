# 05. 简历与博客写法草稿

## 1. 简历怎么写更像“做过项目”

### 中文版写法

可以直接参考下面三条，别三条全抄，挑适合你的：

1. 基于 `Hadoop 3.4.3` 自主搭建双子集群 `YARN Federation` 学习环境，完成 `Router + Federation StateStore + AMRMProxy + Kerberos` 联调与验证。
2. 围绕 `prod/dr` 多数据中心作业统一提交与统一查询场景，梳理 `applicationId -> home sub-cluster` 路由机制，并输出联邦化接入与排障手册。
3. 编写 `Kerberos principal/keytab`、`Router queue policy`、`LinuxContainerExecutor`、`ZooKeeper StateStore` 等配置模板和冒烟脚本，沉淀从 0 到 1 部署文档。

### 英文版写法

1. Built a hands-on `Hadoop 3.4.3 YARN Federation` lab with `Router`, `Federation State Store`, `AMRMProxy`, and Kerberos-enabled security.
2. Studied a `prod/dr` multi-cluster submission and status-query scenario, and verified the `applicationId -> home sub-cluster` routing path through the federation router.
3. Produced deployment notes, configuration templates, and troubleshooting guides for Kerberos keytabs, Router policies, secure NodeManager execution, and federation state storage.

## 2. 博客标题建议

### 标题 1

`为什么 prod 的 YARN appId 到 dr 查不到？一次把 YARN Federation 和 Kerberos 讲清楚`

### 标题 2

`Hadoop 3.4.3 实战：从 0 到 1 搭建带 Kerberos 的 YARN Federation`

### 标题 3

`双机房 prod/dr 场景下，YARN Federation 到底能解决什么，不能解决什么`

## 3. 博客正文目录建议

### 方案一：偏实战

1. 业务背景：为什么两个 YARN 集群的 appId 不互通
2. YARN Federation 架构速览
3. 为什么 Federation 不是“运行中任务灾备”
4. 实验环境规划
5. Kerberos 从 0 到 1
6. Hadoop 3.4.3 联邦配置
7. Router 权重策略与统一提交
8. 冒烟验证
9. 常见踩坑
10. 回到真实生产场景

### 方案二：偏思考

1. 先讲误区：跨集群直接查 appId 为什么不成立
2. 再讲答案：Router + StateStore 如何建立统一视图
3. 最后讲边界：为什么跨机房灾备不能只靠 Federation

## 4. 面试里容易被追问的问题

### 4.1 你为什么觉得 Federation 值得研究

回答方向：

1. 因为它能统一入口
2. 能统一应用查询路径
3. 能为多子集群扩展和调度策略提供基础
4. 但我也明确知道它不是完整 DR 方案

### 4.2 为什么你不一开始就上 RM HA

回答方向：

1. 学习项目先拆分问题
2. 先验证联邦核心闭环
3. 再叠加 HA，能显著降低排障复杂度

### 4.3 你做这个项目最大的收获是什么

回答方向：

1. 理清了 YARN 联邦的控制面结构
2. 真正理解了 Kerberos 下 Hadoop 的 principal/keytab/auth_to_local
3. 认识到“统一入口”和“容灾恢复”是两类问题

## 5. 你可以直接拿去用的项目介绍短版

`围绕工作中的 prod/dr 双 YARN 集群状态查询割裂问题，搭建了 Hadoop 3.4.3 YARN Federation 学习环境，完成 Kerberos 化、Router 统一入口、联邦队列策略与排障手册整理，验证了统一提交与统一查询的可行路径，并梳理了联邦方案在跨机房容灾场景中的适用边界。`

## 6. 建议你后面再补的内容

如果你想让这个项目更像“做过”，可以再追加：

1. 一篇真正的搭建记录博客
2. 一张 Router/StateStore/AMRMProxy 架构图
3. 一份 `docker-compose` 或 `Vagrant + Ansible` 版实验环境
4. 一个“从非 HA 演进到 RM HA”的补充文档
