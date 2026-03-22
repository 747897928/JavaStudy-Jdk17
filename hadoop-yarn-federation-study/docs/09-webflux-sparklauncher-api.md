# 09. WebFlux + SparkLauncher + YARN 状态查询接口设计

这篇是专门贴合你们现在的接入模式写的：

1. Java Web 服务
2. 基于 WebFlux
3. 用 `SparkLauncher` 提交 Spark on YARN
4. 提交后不等待作业跑完
5. 立刻返回 `appId`
6. 再提供一个状态查询接口去查 YARN

## 1. 先说一个关键事实

Spark 官方 `SparkLauncher` JavaDoc 明确写了两件重要的事：

1. `startApplication(...)` 是更推荐的启动方式，因为相对 `launch()` 有更好的控制能力。
2. `SparkAppHandle.getAppId()` 返回的是 application ID，`or null if not yet known`。

这直接决定了你的接口设计不能天真地假设：

`SparkLauncher 一返回，appId 就一定已经拿到了`

## 2. 这意味着什么

如果你们产品接口契约写死成：

1. 调一次提交接口
2. 同步返回 `appId`

那服务端就只能做下面二选一：

### 方案 A：短时间轮询 `handle.getAppId()`

优点：

1. 调用方体验接近你们现状
2. 很多时候几秒内能拿到 `appId`

缺点：

1. 不是 100% 保证
2. 高并发下要小心线程占用和等待堆积

### 方案 B：先返回 `submissionId`，再异步补 `appId`

优点：

1. 设计更稳
2. 更符合 `SparkLauncher` 的真实语义

缺点：

1. 要改调用方契约
2. 业务方心智要适应

## 3. 我对你们场景的建议

如果你们现在已经把“同步返回 appId”做成既有契约，我建议用折中版：

1. 提交接口内部最多等 `3s ~ 10s`
2. 如果 `appId` 在窗口内拿到，就返回 `appId`
3. 如果没拿到，就返回：
   1. `submissionId`
   2. 当前 launcher 状态
   3. 一个“稍后查询”的接口地址

这样比强行无限等待靠谱得多。

## 4. 推荐的接口模型

### 4.1 提交接口

```http
POST /api/spark/jobs
```

请求体示例：

```json
{
  "jobName": "daily-etl",
  "mainClass": "com.example.jobs.DailyEtlJob",
  "appResource": "hdfs:///apps/jobs/daily-etl.jar",
  "appArgs": ["2026-03-22"],
  "queue": "root.batch",
  "driverMemory": "2g",
  "executorMemory": "4g",
  "executorInstances": 2
}
```

返回体建议：

```json
{
  "submissionId": "sub-20260322-0001",
  "applicationId": "application_1742640000000_0007",
  "launcherState": "SUBMITTED",
  "statusQueryUri": "/api/spark/jobs/sub-20260322-0001/status"
}
```

### 4.2 状态查询接口

```http
GET /api/spark/jobs/{submissionId}/status
```

返回体建议：

```json
{
  "submissionId": "sub-20260322-0001",
  "applicationId": "application_1742640000000_0007",
  "launcherState": "RUNNING",
  "yarnState": "RUNNING",
  "finalStatus": "UNDEFINED",
  "trackingUrl": "http://yarn-router.company.com:8089/cluster/app/application_..."
}
```

## 5. 推荐的服务内部分层

### 5.1 Submission API 层

负责：

1. 参数校验
2. 鉴权
3. 调用提交服务
4. 返回 `submissionId/appId`

### 5.2 SparkLauncher 提交层

负责：

1. 组装 `SparkLauncher`
2. 设置 `HADOOP_CONF_DIR`
3. 设置 `master=yarn`
4. 设置 `deployMode=cluster`
5. 设置 `queue/principal/keytab`
6. 调用 `startApplication`

### 5.3 Handle 状态监听层

负责：

1. 监听 `SparkAppHandle.Listener`
2. 捕获 `stateChanged/infoChanged`
3. 尝试提取 `appId`
4. 把状态异步落库

### 5.4 YARN 查询层

负责：

1. 根据 `submissionId` 找到 `appId`
2. 通过 Router 去查 YARN 应用状态
3. 返回统一 DTO

## 6. 最关键的工程实现点

### 6.1 提交不要阻塞 WebFlux event loop

`SparkLauncher.startApplication()` 和后续短轮询都不该跑在 Netty event loop 上。

建议：

1. 用专门的 submission executor
2. 在 Reactor 里包到 `Mono.fromCallable(...)`
3. 丢到 `Schedulers.boundedElastic()` 或自定义线程池

### 6.2 `SparkAppHandle.Listener` 里不要做重活

Spark 官方 JavaDoc 明确说了：

1. listener 会在处理应用更新的线程上被调用
2. listener 应避免阻塞或长时间运行

所以 listener 里应该只做：

1. 取 `appId`
2. 取 state
3. 投递异步更新任务

不要在 listener 里直接写重数据库事务、远程 HTTP 调用、复杂日志处理。

### 6.3 `appId` 需要独立落库

因为 `getAppId()` 初期可能为空。

推荐表结构至少有：

1. `submission_id`
2. `application_id`
3. `launcher_state`
4. `yarn_state`
5. `final_status`
6. `tracking_url`
7. `created_at`
8. `updated_at`

## 7. 你们这种“支持 Kerberos”的提交侧应该怎么做

你们既然是服务端统一提交，就不要依赖“每个调用方自己先 `kinit`”。

更稳的做法是服务端统一配置：

1. Router client conf
2. `sparkuser` 或受控服务账号的 `principal`
3. 对应 `keytab`

Spark 官方安全文档明确说明：

1. 给 Spark 提供 `principal` 和 `keytab`
2. 应用可以维持有效的 Kerberos 登录并持续续租相关凭据

## 8. Router 接入怎么和 SparkLauncher 结合

本质上和命令行 `spark-submit` 一样，关键仍然是客户端配置目录：

1. `HADOOP_CONF_DIR`
2. `YARN_CONF_DIR`
3. 里面的 `yarn-site.xml` 指向 Router，而不是某个 RM

也就是说，`SparkLauncher` 不是替代 Router 的东西，它只是你们 Java 服务里调用 `spark-submit` 逻辑的编程接口。

## 9. 一个贴近你们场景的实现建议

### 提交流程

1. WebFlux controller 收到请求
2. 生成 `submissionId`
3. 提交服务在独立线程池里调用 `SparkLauncher.startApplication(listener...)`
4. listener 异步更新 `appId/state`
5. controller 在限定时间内轮询 `handle.getAppId()`
6. 拿到就返回 `appId`
7. 拿不到就返回 `submissionId + PENDING_APP_ID`

### 查询流程

1. 根据 `submissionId` 查本地表
2. 如果还没拿到 `appId`，先返回 launcher 侧状态
3. 如果已有 `appId`，再经 Router 查 YARN
4. 返回统一任务状态 DTO

## 10. 为什么我不建议“只靠内存保存 handle”

因为 Web 服务重启后，内存里的 `SparkAppHandle` 就没了。

你们真正稳定的查询依据应该是：

1. `submissionId`
2. `applicationId`
3. 通过 Router 查询 YARN

Handle 更像“提交过程中的短期控制对象”，不是长期状态数据库。

## 11. 建议的返回状态模型

### 提交阶段

1. `ACCEPTED`
2. `SUBMITTED`
3. `APP_ID_PENDING`
4. `FAILED_TO_LAUNCH`

### YARN 阶段

1. `NEW`
2. `NEW_SAVING`
3. `SUBMITTED`
4. `ACCEPTED`
5. `RUNNING`
6. `FINISHED`
7. `FAILED`
8. `KILLED`

## 12. 代码样例

项目里补了一份直接放在模块根级 `src/` 下的代码骨架：

`src/main/java/com/aquarius/wizard/study/sparklauncher`

里面包含：

1. 提交请求 DTO
2. 提交响应 DTO
3. `SparkLauncherSubmissionService`
4. `YarnApplicationStatusService`
5. `SparkJobController`

## 13. 最后的结论

对你们这种 `WebFlux + SparkLauncher + Kerberos + 返回 appId + 查状态接口` 的模式，最重要的是三点：

1. 提交入口必须统一加载 Router client conf
2. 不要假设 `getAppId()` 一定同步可得
3. 长期状态查询要依赖 `submissionId + appId + Router 查询`，不是依赖进程内 handle

## 参考资料

1. SparkLauncher JavaDoc: https://spark.apache.org/docs/latest/api/java/org/apache/spark/launcher/SparkLauncher.html
2. SparkAppHandle JavaDoc: https://spark.apache.org/docs/latest/api/java/org/apache/spark/launcher/SparkAppHandle.html
3. Spark on YARN: https://spark.apache.org/docs/latest/running-on-yarn.html
4. Spark Security: https://spark.apache.org/docs/latest/security.html
