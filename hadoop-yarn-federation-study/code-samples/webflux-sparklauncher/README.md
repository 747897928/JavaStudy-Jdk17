# WebFlux SparkLauncher Sample

这不是一个完整可运行模块，而是一组贴近真实项目分层的代码骨架，方便你后面迁到自己的 WebFlux 服务里。

## 包含内容

1. `LaunchSparkJobRequest`
2. `LaunchSparkJobResponse`
3. `SparkLauncherSubmissionService`
4. `YarnApplicationStatusService`
5. `SparkJobController`

## 设计目标

1. 提交接口尽快返回
2. 优先返回 `appId`
3. 如果 `appId` 暂时未知，则返回 `submissionId`
4. 状态查询统一走 YARN Router

## 你真正落地时还要补的东西

1. 数据库存储
2. 提交记录表
3. 鉴权
4. 限流
5. 审计
6. 失败重试和幂等
