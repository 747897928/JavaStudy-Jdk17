# Hadoop YARN Federation Study

这个 module 用来存放 `Hadoop 3.4.3 + YARN Federation + Kerberos` 的学习笔记、配置模板、脚本示例和排障手册，目标是做成一个可以写进简历、也方便继续扩写成博客的学习项目。

## 你现在遇到的问题，先说结论

你们现在的现象很典型：

1. `prod` 和 `dr` 是两套独立 YARN 集群。
2. Spark 任务如果直接提交到 `prod` 的 RM，拿到的 `application_*` 只在 `prod` 这个 RM 视角内可见。
3. 你再去 `dr` 查这个 `appId`，`dr` 不认识，这是正常现象，不是故障。

`YARN Federation` 能解决的是：

1. 通过 `Router` 提供统一入口。
2. Router 在提交应用时决定它的 `home sub-cluster`。
3. Router 把 `application -> sub-cluster` 的映射写进 `Federation State Store`。
4. 之后你再通过任意 Router 查这个 `appId`，Router 能把请求路由到正确的 RM。

`YARN Federation` 不能自动解决的是：

1. 你以前已经直接提交到 `prod RM` 的任务，不会自动“变成全局可见”。
2. 正在 `prod` 跑的 Spark 应用，不会因为 `prod` 整个机房挂掉就自动迁移到 `dr` 继续跑。
3. 它不是“跨机房运行中作业灾备”的完整替代方案，更像是“统一入口 + 多子集群路由 + 跨子集群调度框架”。

## 这个学习模块包含什么

1. [业务背景与架构判断](./docs/01-background-and-architecture.md)
2. [Kerberos 入门速读](./docs/00-kerberos-primer.md)
3. [CentOS 风格 Kerberos 从 0 到 1](./docs/02-centos-kerberos-bootstrap.md)
4. [Hadoop 3.4.3 联邦实验环境落地步骤](./docs/03-hadoop-3.4.3-federation-lab.md)
5. [常见错误与排障手册](./docs/04-troubleshooting.md)
6. [简历与博客写法草稿](./docs/05-resume-and-blog-notes.md)
7. [Linux 命令入门与脚本拆解](./docs/06-linux-command-primer.md)
8. [Spark 统一走 Router 的提交方案](./docs/07-spark-router-submission.md)
9. [RM HA、多 Router、SQL StateStore 的生产化改造路径](./docs/08-production-grade-roadmap.md)
10. [WebFlux + SparkLauncher + YARN 状态查询接口设计](./docs/09-webflux-sparklauncher-api.md)
11. [Vagrant + Ansible Lab](./lab/vagrant-ansible/README.md)
12. `conf-templates/` 下的配置模板
13. `scripts/` 下的主机准备、principal/keytab 生成、联邦冒烟脚本

## 我建议你怎么学，最稳

分两阶段，不要一上来就追生产级全家桶：

1. 第一阶段先跑通 `非 HA 子集群 + ZK StateStore + Kerberos + 1 Router + 2 SubCluster`。
2. 第二阶段再补 `RM HA + 多 Router + SQL StateStore + KDC 副本 + ZooKeeper 安全`。

这样做的原因很现实：

1. 你要先把“联邦能不能把 prod/dr 的统一提交与统一查询打通”这个核心闭环跑通。
2. 如果一开始就上 `Kerberos + RM HA + Router + ZK + SQL + 跨机房网络`，排障面太大，不利于写博客。

## 这个项目里我刻意强调的几个边界

1. Apache Hadoop 3.4.3 官方 YARN Federation 文档在 2026-02-13 发布的版本里明确写了一个前提：`not looking to federate across DC yet`。也就是说，官方设计出发点更偏“大集群拆成多个子集群做扩展”，不是天然为“跨数据中心灾备”设计。
2. 同一份官方文档还明确假设底层依赖 `HDFS federation or equivalently scalable DFS solutions`。所以如果你们 prod/dr 的存储层完全隔离，没有统一数据访问路径，那联邦只解决控制面的一部分，不解决数据面的统一访问。
3. 如果你们的真实诉求只是“无论应用在哪个集群提交，我都能用一个统一入口查状态”，那关键点不是 RM 互通，而是“以后统一走 Router 提交和查询”。

## 目录约定

```text
hadoop-yarn-federation-study
├── README.md
├── docs
│   ├── 00-kerberos-primer.md
│   ├── 01-background-and-architecture.md
│   ├── 02-centos-kerberos-bootstrap.md
│   ├── 03-hadoop-3.4.3-federation-lab.md
│   ├── 04-troubleshooting.md
│   ├── 05-resume-and-blog-notes.md
│   ├── 06-linux-command-primer.md
│   ├── 07-spark-router-submission.md
│   ├── 08-production-grade-roadmap.md
│   └── 09-webflux-sparklauncher-api.md
├── conf-templates
│   ├── capacity-scheduler.xml
│   ├── container-executor.cfg
│   ├── core-site.xml
│   ├── federation-machine-list.txt
│   ├── federation-weights.xml
│   ├── hdfs-site.xml
│   ├── kadm5.acl
│   ├── kdc.conf
│   ├── krb5.conf
│   ├── mapred-site.xml
│   ├── spark-defaults-router.conf
│   ├── yarn-site-client.xml
│   ├── yarn-site-prod-ha.xml
│   ├── yarn-site-prod.xml
│   ├── yarn-site-dr-ha.xml
│   ├── yarn-site-dr.xml
│   ├── yarn-site-router-sql.xml
│   └── yarn-site-router.xml
├── lab
│   └── vagrant-ansible
│       ├── README.md
│       ├── Vagrantfile
│       └── ansible
├── code-samples
│   └── webflux-sparklauncher
│       ├── README.md
│       └── src
└── scripts
    ├── create-service-principals.sh
    ├── prepare-host-dirs.sh
    ├── smoke-test-federation.sh
    └── spark-submit-router.sh
```

## 推荐实验拓扑

最小可讲清楚、也便于写博客的拓扑：

1. `kdc1.lab.example.com`: KDC
2. `nn1.lab.example.com`: NameNode + JobHistory + ZooKeeper
3. `prod1.lab.example.com`: `SC-PROD` 的 RM + NM + DN
4. `dr1.lab.example.com`: `SC-DR` 的 RM + NM + DN
5. `router1.lab.example.com`: Router + Client

## 推荐阅读顺序

如果你对 Kerberos 和 Linux 都比较陌生，建议按这个顺序读：

1. [Kerberos 入门速读](./docs/00-kerberos-primer.md)
2. [Linux 命令入门与脚本拆解](./docs/06-linux-command-primer.md)
3. [CentOS 风格 Kerberos 从 0 到 1](./docs/02-centos-kerberos-bootstrap.md)
4. [Hadoop 3.4.3 联邦实验环境落地步骤](./docs/03-hadoop-3.4.3-federation-lab.md)
5. [常见错误与排障手册](./docs/04-troubleshooting.md)
6. [Spark 统一走 Router 的提交方案](./docs/07-spark-router-submission.md)
7. [RM HA、多 Router、SQL StateStore 的生产化改造路径](./docs/08-production-grade-roadmap.md)
8. [WebFlux + SparkLauncher + YARN 状态查询接口设计](./docs/09-webflux-sparklauncher-api.md)
9. 如果要动手起实验环境，再看 [Vagrant + Ansible Lab](./lab/vagrant-ansible/README.md)

## 你后面写博客可以直接用的主线

1. 为什么 `prod appId` 在 `dr` 查不到是正常现象。
2. Router + StateStore 是怎么把“统一提交”和“统一查询”做出来的。
3. Kerberos 化以后哪些组件必须有 principal/keytab。
4. 从 0 到 1 怎么先搭非 HA 联邦，再平滑升级到更接近生产的形态。

## 官方资料

1. Apache Hadoop 3.4.3 YARN Federation: https://hadoop.apache.org/docs/r3.4.3/hadoop-yarn/hadoop-yarn-site/Federation.html
2. Apache Hadoop Secure Mode 3.4.1: https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SecureMode.html
3. Apache Hadoop Single Node Setup 3.4.3: https://hadoop.apache.org/docs/r3.4.3/hadoop-project-dist/hadoop-common/SingleCluster.html
4. MIT Kerberos Admin Guide: https://web.mit.edu/Kerberos/krb5-latest/doc/admin/install_kdc.html
