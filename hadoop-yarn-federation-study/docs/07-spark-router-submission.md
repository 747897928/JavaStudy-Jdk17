# 07. Spark 统一走 Router 的提交方案

这篇专门解决你工作里的那个关键问题：

`prod` 和 `dr` 两套 YARN 独立存在时，怎么把 Spark 提交和状态查询统一收口到 Router。

## 1. 先说核心判断

如果你们继续让应用直连各自的 RM：

1. `prod` 提交的 app 只在 `prod RM` 视角内可见
2. `dr` 提交的 app 只在 `dr RM` 视角内可见
3. 后面再去做“统一查询”会很别扭

所以真正要改的不是“appId 格式”，而是“提交入口”。

## 2. 官方依据，先对齐

Apache Hadoop Federation 官方文档的核心思路是：

1. 客户端通过 Router 提交应用
2. Router 决定 `home sub-cluster`
3. Router 把映射写进联邦 StateStore
4. 后续再通过 Router 查询应用状态

Spark 官方 `Running on YARN` 文档又明确说明：

1. Spark on YARN 依赖 `HADOOP_CONF_DIR` 或 `YARN_CONF_DIR`
2. `spark-submit --master yarn` 最终还是走这套 Hadoop/YARN 客户端配置

所以结论很直接：

只要你的 Spark 提交环境加载的是“指向 Router 的客户端配置”，Spark 就会把作业提交到 Router，而不是直连某个 RM。

## 3. 目标接入架构

推荐你把生产目标设计成下面这层结构：

```text
spark-submit / 调度平台 / 网关机
            |
            v
    hadoop-router-client-conf
            |
            v
    yarn-router.company.com:8050
            |
            v
       Router 集群
            |
            v
   SC-PROD RM HA / SC-DR RM HA
```

关键点不是每台 Spark 客户端都知道所有 RM，而是：

1. 它只认 Router 的统一入口
2. Router 再去决定落到哪个 sub-cluster

## 4. 你们应该怎么改 Spark 提交

### 4.1 不要再让 Spark 客户端读直连 RM 的 `yarn-site.xml`

最忌讳的情况是：

1. 一部分机器读 `prod` 的 `yarn-site.xml`
2. 一部分机器读 `dr` 的 `yarn-site.xml`
3. 大家以为已经联邦化，实际上还是分裂入口

### 4.2 单独准备一套 Router 客户端配置目录

比如：

```text
/etc/hadoop-router-client
```

里面只放客户端需要的：

1. `core-site.xml`
2. `hdfs-site.xml`
3. `mapred-site.xml`
4. `yarn-site.xml`

这份 `yarn-site.xml` 要来自联邦 Router 客户端模板，也就是 [yarn-site-client.xml](../conf-templates/yarn-site-client.xml)。

### 4.3 所有 Spark 提交统一通过环境变量指向这份配置

```bash
export HADOOP_CONF_DIR=/etc/hadoop-router-client
export YARN_CONF_DIR=/etc/hadoop-router-client
```

这一步是关键中的关键。

## 5. 推荐的 Spark 提交规范

### 5.1 统一入口脚本

项目里补了一份脚本：

[spark-submit-router.sh](../scripts/spark-submit-router.sh)

它的目的不是炫技，而是做三件事：

1. 强制所有提交都带 Router client conf
2. 强制队列名可控
3. 为 Kerberos 的 `principal/keytab` 留统一入口

### 5.2 统一 Spark 默认配置

项目里补了一份模板：

[spark-defaults-router.conf](../conf-templates/spark-defaults-router.conf)

建议你把 Router 接入做成平台标准，而不是让每个业务方自己拼参数。

### 5.3 队列策略

和联邦队列策略一起使用时，建议这样分：

1. `root.batch`
   平时默认队列，主要进 `SC-PROD`
2. `root.recovery`
   演练或切流队列，优先进 `SC-DR`

这样做比临时改 RM 地址干净得多。

## 6. Kerberos 下 Spark 还要注意什么

Spark 官方安全文档明确建议：

1. 长任务可以通过 `--principal` 和 `--keytab` 提交
2. Spark 能代表应用续租 Kerberos 凭据

所以你在生产里不要只靠“先手工 `kinit` 一下”。

更稳的方式是：

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --principal sparkuser@EXAMPLE.COM \
  --keytab /etc/security/keytabs/sparkuser.keytab \
  ...
```

## 7. 我建议的接入步骤

### 第一步：建立客户端配置标准件

把 Router client conf 做成标准安装包，发到：

1. Spark 网关机
2. 调度平台 worker
3. 需要手工提交的跳板机

### 第二步：灰度一个队列

先让少量作业走：

1. Router
2. `root.batch`
3. `SC-PROD` 为主的权重

### 第三步：强制收口

把所有历史直连 RM 的配置清掉：

1. Shell 脚本
2. 调度平台连接配置
3. Docker 镜像里的旧 `yarn-site.xml`
4. 业务方私有提交脚本

### 第四步：增加查询与审计

要求所有状态查询也统一走 Router：

1. `yarn application -status`
2. Router Web UI
3. 调度平台里的 YARN 状态拉取逻辑

## 8. 回滚方案怎么设计

上线前必须想好回滚。

最简单的回滚不是“改代码”，而是：

1. 保留旧版直连 RM 的客户端配置目录
2. Router client conf 独立目录部署
3. 通过环境变量或软链切换

例如：

```text
/etc/hadoop-client-prod
/etc/hadoop-client-router
```

这样出问题时，只要切回旧目录即可。

## 9. 你们 prod/dr 场景最该防的坑

1. Spark 客户端以为在走 Router，实际上镜像里还带着旧 `yarn-site.xml`
2. 同一个平台的不同 worker 节点，加载的 Hadoop conf 不一致
3. 只改了提交入口，没改状态查询入口
4. 长跑 Spark 作业没带 `principal/keytab`，凭据过期后失败
5. 队列权重没管理好，导致本应落 `prod` 的任务大量进入 `dr`

## 10. 一个可执行的最小样例

```bash
export HADOOP_CONF_DIR=/etc/hadoop-router-client
export YARN_CONF_DIR=/etc/hadoop-router-client

$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue root.batch \
  --principal sparkuser@EXAMPLE.COM \
  --keytab /etc/security/keytabs/sparkuser.keytab \
  --class com.example.jobs.DemoJob \
  demo-job.jar
```

## 11. 这篇的落地结论

对于你们的场景，真正要推动的是：

1. Spark 提交统一走 Router
2. Spark/YARN 状态查询统一走 Router
3. 队列策略来决定 `prod/dr` 去向
4. 不再让业务方手工选择 RM 地址

## 参考资料

1. Apache Hadoop YARN Federation: https://hadoop.apache.org/docs/r3.4.3/hadoop-yarn/hadoop-yarn-site/Federation.html
2. Apache Spark Running on YARN: https://spark.apache.org/docs/latest/running-on-yarn.html
3. Apache Spark Security: https://spark.apache.org/docs/latest/security.html
