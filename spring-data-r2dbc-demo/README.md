# Spring Data R2DBC Demo

一个偏“简历项目”的学习模块，重点演示下面这条响应式链路：

- `Spring WebFlux` 负责响应式 HTTP 接入
- `Spring Data R2DBC` 负责响应式数据库访问
- `WebClient` 负责非阻塞的下游 HTTP 调用
- `PostgreSQL` 使用单机或主从故障转移拓扑

这个模块特意避免了在 WebFlux 请求线程里混用 JDBC / `block()` / 阻塞式 SDK，核心目的是把“响应式链路为什么要全链路异步”这件事做成一个可运行、可写进简历、可拿来做笔记的项目。

## 项目亮点

- 双 `ConnectionFactory`：写流量指向 primary，读流量优先 secondary
- R2DBC 连接池：与 WebFlux/Netty 配套，不把阻塞式数据库访问塞进 event-loop
- Writer 连接借出时做角色校验：主从切换后自动丢弃旧 primary 连接，减少应用重启需求
- `WebClient` 调用模拟下游风控/运费服务，全链路保持 `Mono` / `Flux`
- 拓扑探针接口：直接告诉你当前读写请求实际连到了哪个 PostgreSQL 节点
- Docker 主从演示：本地没装 PostgreSQL 也能直接起一个 HA 拓扑

## 业务场景

Demo 用的是一个简化的“响应式订单中心”：

- `POST /api/orders`：创建订单
- `GET /api/orders`：从读库列出订单
- `GET /api/orders/{orderNo}`：从读库查订单明细
- `GET /api/topology`：查看 writer / reader 当前落到哪个 PostgreSQL 节点

订单创建时，会先用 `WebClient` 去调用一个本地模拟的下游风控/运费服务，再把订单写入 PostgreSQL。

## 源码导航

如果你是拿这个模块做学习项目，建议按下面顺序看代码：

- 启动入口：[DemoApplication.java](./src/main/java/com/aquarius/wizard/springdatar2dbcdemo/DemoApplication.java)
- 数据库与主从切换核心配置：[ReactiveDatabaseConfig.java](./src/main/java/com/aquarius/wizard/springdatar2dbcdemo/config/ReactiveDatabaseConfig.java)
- 数据库参数与连接池参数：[DemoDatabaseProperties.java](./src/main/java/com/aquarius/wizard/springdatar2dbcdemo/config/DemoDatabaseProperties.java)
- 写链路：先调下游再写库：[OrderCommandService.java](./src/main/java/com/aquarius/wizard/springdatar2dbcdemo/service/OrderCommandService.java)
- 读链路：查询默认走 reader：[OrderQueryService.java](./src/main/java/com/aquarius/wizard/springdatar2dbcdemo/service/OrderQueryService.java)
- 主从拓扑探针：[DatabaseTopologyService.java](./src/main/java/com/aquarius/wizard/springdatar2dbcdemo/service/DatabaseTopologyService.java)
- 模拟下游 WebClient 调用目标：[PartnerRiskStubController.java](./src/main/java/com/aquarius/wizard/springdatar2dbcdemo/controller/PartnerRiskStubController.java)

主从切换最关键的源码位置：

- 新建物理连接时按主从角色选节点：`writer-url` / `reader-url`
- 旧 writer 连接借出时做角色校验：`ReactiveDatabaseConfig#ensureWriterConnection`
- 查看当前连接落点：`DatabaseTopologyService#inspect`

## 快速启动

### 1. 单机 PostgreSQL

如果你只是先学习 R2DBC 基础，可以直接起一个单机 PostgreSQL：

```bash
docker run --name r2dbc-demo-pg ^
  -e POSTGRES_USER=demo ^
  -e POSTGRES_PASSWORD=demo ^
  -e POSTGRES_DB=reactive_order_demo ^
  -p 5432:5432 ^
  -d postgres:17
```

然后启动应用：

```bash
mvn -pl spring-data-r2dbc-demo spring-boot:run
```

### 2. 主从/故障转移演示

如果你要学习主从切换和多节点 URL，先启动 HA 拓扑：

```bash
docker compose -f spring-data-r2dbc-demo/docker-compose.postgres-ha.yml up -d
```

再用 `ha` profile 启动应用：

```bash
mvn -pl spring-data-r2dbc-demo spring-boot:run -Dspring-boot.run.profiles=ha
```

`ha` profile 下的连接串使用了：

- 写库：`targetServerType=PRIMARY`
- 读库：`targetServerType=PREFER_SECONDARY`

同时连接池额外做了一层保护：

- 新建物理连接时，由 `r2dbc-postgresql` 按 `targetServerType` 选择合适节点
- 从池里借出 writer 连接时，再执行一次角色校验
- 如果发现这个连接已经落到 standby / read-only 节点，立即废弃并重建

这比“只配 failover URL”更适合学习真实的主从切换场景，因为连接池里原本缓存的连接不会自己重新选主。

## 试运行

### 创建订单

```bash
curl -X POST http://localhost:8083/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"customerName\":\"Cathy\",\"customerTier\":\"PLATINUM\",\"skuCode\":\"PG-HA-CASE\",\"quantity\":3,\"unitPrice\":199.00,\"remark\":\"learning r2dbc\"}"
```

### 查看订单列表

```bash
curl http://localhost:8083/api/orders?limit=10
```

### 查看数据库拓扑

```bash
curl http://localhost:8083/api/topology
```

如果 HA 拓扑正常，通常会看到：

- `writer.inRecovery = false`
- `reader.inRecovery = true`

这说明写连接落到了 primary，读连接优先落到了 standby。

## 关于“不重启自动切换”

这个 Demo 现在支持的是：

- PostgreSQL 发生主从角色切换后，应用不重启，后续新借出的 writer 连接会尽量跟随新的 primary
- reader 连接会继续按 `PREFER_SECONDARY` 策略获取可用节点

但你要注意边界：

- 切换瞬间正在执行的请求，仍然可能失败
- 已经拿到手、正在使用的旧连接，不可能被驱动“瞬间变身”为新主库连接
- 真正能切的是“下一次获取连接时重新识别拓扑”，不是“所有在途事务零损切换”

这也是为什么很多生产环境还会配合：

- PgBouncer / HAProxy / VIP
- 应用层重试
- 幂等键 / 去重
- 主从一致性治理

## 参考资料

- R2DBC PostgreSQL 多 host / `targetServerType` / failover 说明：https://github.com/pgjdbc/r2dbc-postgresql
- R2DBC Pool 连接池与生命周期配置说明：https://github.com/r2dbc/r2dbc-pool
- Spring Data R2DBC Repository 与多 `R2dbcEntityOperations` 说明：https://docs.spring.io/spring-data/relational/reference/r2dbc/repositories.html

## 推荐阅读

- [学习笔记](./docs/learning-notes.md)
- [PostgreSQL 本地安装与主从演示](./docs/postgres-local-setup.md)

## 简历写法示例

- 基于 Spring WebFlux、Spring Data R2DBC、WebClient 搭建响应式订单 Demo，实现 HTTP 调用、数据库访问全链路非阻塞。
- 设计双连接池读写分离方案，PostgreSQL 读库优先 secondary、写库固定 primary，并提供拓扑探针验证故障转移效果。
- 使用 Docker Compose 搭建 PostgreSQL 主从演示环境，沉淀 WebFlux 阻塞风险、连接池耗尽与主从一致性取舍的学习笔记。
