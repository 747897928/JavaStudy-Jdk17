# 模块总览与交接说明

这份文档是给“后续接手这个模块的人或 AI”看的，不假设对话上下文仍然存在。

## 1. 这个模块是干什么的

`spring-data-r2dbc-demo` 是一个学习型、可写进简历的 Spring Boot 模块，目标是演示：

- WebFlux 入口是响应式的
- 下游 HTTP 调用使用 `WebClient`，而不是阻塞式客户端
- PostgreSQL 数据访问使用 Spring Data R2DBC，而不是 JDBC
- 主从拓扑下，读写连接分离，并观察切换后的连接行为

它不是一个追求业务复杂度的项目，而是一个“把响应式链路、连接池、主从切换这些概念放在一起讲清楚”的项目。

## 2. 为什么会有这个模块

这个模块来自一个真实问题背景：

- 在 WebFlux 项目里混用了阻塞式数据库访问
- 日志里出现过类似“连接池拿不到连接”的现象
- 希望学习为什么响应式项目最好让 HTTP 调用和数据库访问都尽量异步化

所以这个模块故意做成：

- Controller 返回 `Mono` / `Flux`
- 下游 HTTP 调用使用 `WebClient`
- 数据库访问使用 R2DBC

## 3. 核心设计

### 3.1 双连接池

- `writerConnectionFactory` 负责写流量
- `readerConnectionFactory` 负责读流量

HA 模式下：

- writer URL 使用 `targetServerType=PRIMARY`
- reader URL 使用 `targetServerType=PREFER_SECONDARY`

### 3.2 为什么不只靠 failover URL

只配 failover URL 不够，因为：

- 驱动只有在“新建物理连接”时才会重新识别主从角色
- 如果前面有连接池，池里已经缓存的旧连接不会自动重新选主

所以模块里额外做了一层 writer 连接借出校验：

- 每次从 writer 池借连接时，检查 `pg_is_in_recovery()` 和 `transaction_read_only`
- 如果这个连接已经不再指向可写主库，立即丢弃，让连接池重建

关键代码位置：

- `ReactiveDatabaseConfig#buildPooledConnectionFactory`
- `ReactiveDatabaseConfig#ensureWriterConnection`

## 4. 建议阅读顺序

1. `README.md`
2. `docs/learning-notes.md`
3. `config/ReactiveDatabaseConfig.java`
4. `config/DemoDatabaseProperties.java`
5. `service/OrderCommandService.java`
6. `service/OrderQueryService.java`
7. `service/DatabaseTopologyService.java`

## 5. 当前已经明确的边界

这个模块支持的是：

- PostgreSQL 主从角色切换后，应用不重启
- 后续新借出的 writer 连接尽量跟随新的 primary
- reader 连接继续按从库优先策略获取

这个模块不承诺的是：

- 切换瞬间在途请求零失败
- 已经借出的旧连接瞬间“自动变成新主库连接”
- 生产级别的高可用兜底全部在客户端完成

如果后续要继续增强，优先考虑：

- 增加重试策略示例
- 增加幂等与去重示例
- 增加故障切换联调脚本
- 增加集成测试容器化验证

## 5.1 当前验证状态

当前已经验证：

- `mvn -pl spring-data-r2dbc-demo test`
- `mvn -pl spring-data-r2dbc-demo -DskipTests package`

当前还没有自动化验证：

- 基于真实 PostgreSQL 主从切换的集成测试
- 切换瞬间在途请求失败率统计
- 长时间运行下连接池角色漂移观察

## 6. 关键接口

- `POST /api/orders`：创建订单，走写链路
- `GET /api/orders`：读库列表查询
- `GET /api/orders/{orderNo}`：读库明细查询
- `GET /api/topology`：查看当前 writer / reader 实际连接到的数据库节点

## 7. 外部参考

- R2DBC PostgreSQL：
  https://github.com/pgjdbc/r2dbc-postgresql
- R2DBC Pool：
  https://github.com/r2dbc/r2dbc-pool
- Spring Data R2DBC：
  https://docs.spring.io/spring-data/relational/reference/r2dbc/repositories.html
