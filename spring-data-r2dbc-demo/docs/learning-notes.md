# Spring Data R2DBC 学习笔记

## 1. 为什么在 WebFlux 里尽量用 R2DBC

WebFlux 的底层是事件循环模型。它擅长处理大量并发连接，但前提是：

- 请求处理链尽量不要阻塞
- 一个请求占用线程的时间要尽量短
- IO 等待要交给异步驱动处理

如果 Controller / Service 仍然走 JDBC 或者在链路中频繁 `block()`，问题就来了：

- Netty event-loop 线程被阻塞
- 请求不能及时继续向后推进
- 下游连接归还变慢
- 连接池等待时间拉长
- 最终出现“连接池拿不到连接”、“请求提前超时/中断”这类现象

你提到的“日志里报 netty 连接池找不到连接，WebFlux 里大量阻塞式 DB 连接导致异步请求提前中断”，本质上就是这个方向的问题。

## 2. 这个 Demo 在演示什么

这个模块刻意把链路拆成三部分：

1. WebFlux Controller
2. WebClient 下游调用
3. R2DBC PostgreSQL 读写

这样你可以清楚看到：

- 入站请求是响应式的
- 出站 HTTP 调用是响应式的
- 数据库访问也是响应式的

也就是说，不只是“Controller 返回 `Mono` / `Flux`”，而是整个 IO 链路都没有故意落回阻塞模型。

## 3. 为什么要做双 ConnectionFactory

这个 Demo 不是只做一个单机 PostgreSQL 连接，而是区分：

- `writerConnectionFactory`：写请求，只连 primary
- `readerConnectionFactory`：读请求，优先连 secondary

这么做的原因有两个：

1. 更贴近真实项目里的主从拓扑
2. 方便把 PostgreSQL 多节点连接和故障转移一起学掉

Spring Data 官方在多数据库场景下建议自己定义多个 `ConnectionFactory` 和 `R2dbcEntityOperations`，再把 repository 或 template 绑定到对应 bean 上。

## 4. PostgreSQL 主从切换下的连接策略

`r2dbc-postgresql` 支持多 host 的 failover URL。这个 Demo 在 `ha` profile 里用了两条连接串：

写库：

```text
r2dbc:postgresql:failover://localhost:5432,localhost:5433/reactive_order_demo?targetServerType=PRIMARY
```

读库：

```text
r2dbc:postgresql:failover://localhost:5432,localhost:5433/reactive_order_demo?targetServerType=PREFER_SECONDARY
```

含义分别是：

- 写连接一定要找 primary
- 读连接优先找 secondary，找不到再回退到其他可用节点

这不等于“完全自动解决所有高可用问题”，但足够用来学习：

- 多 host 连接 URL
- 主从角色识别
- 读写连接池分离
- 故障转移后的连接重建

### 4.1 只配 failover URL 还不够

这个点很关键，也是你这次追问的重点。

如果应用没有连接池，每次都重新 `ConnectionFactory.create()` 建物理连接，那么主从切换后，驱动会在新建连接时重新判断要连哪个节点。

但真实项目一般都有连接池。于是会出现一个问题：

- 主从切换前，writer 池里已经缓存了一批连接
- 切换后，这些连接可能还活着
- 如果旧 primary 现在变成了 standby，这些旧连接就不再适合继续做写流量

所以“驱动支持 failover URL”和“应用在主从切换后无重启继续稳定写入”不是同一个层面的问题。

这个 Demo 现在做了两层事：

1. 用 failover URL 让新建物理连接能识别 `PRIMARY` / `PREFER_SECONDARY`
2. 在 writer 连接每次从池里借出时，额外检查 `pg_is_in_recovery()` 和 `transaction_read_only`

如果发现借出来的是一个已经不再可写的旧连接，就直接丢弃，逼连接池重新拿新的物理连接。

这就是你同事提到 `connectionFactory.create()` 的那部分“记忆里大方向是对的”，但还缺了一个现实条件：

- 只有当应用真的要重新建连接时，驱动才会重新选主
- 如果中间有池，池里现成连接不会自动重新选主
- 所以必须配合连接池淘汰/校验策略

## 5. 读写分离最大的坑：一致性

主从复制通常是最终一致，不是强一致。

所以创建订单后，如果你立刻走“读库查询订单明细”，理论上可能出现：

- 刚写成功
- 读库还没追平
- 明细接口一瞬间查不到

这类问题是读写分离必谈的话题。真实项目里常见做法包括：

- 关键读请求回 primary
- 对刚写后的查询走短时间粘 primary
- 用业务版本号 / 事件确认复制追平
- 在接口语义上接受最终一致

这个 Demo 的查询默认走 reader，就是为了把这个点暴露出来，方便你在笔记和面试里讲清楚。

## 6. 为什么还要保留 WebClient

很多人说“我 Controller 返回 `Mono` 了，所以我就是响应式了”，这不够。

如果中间又调用了：

- `RestTemplate`
- `OpenFeign` 阻塞客户端
- JDBC
- 阻塞式 Redis / MQ SDK

那只是表面响应式，内里还是阻塞。

所以这个 Demo 在创建订单时额外调用一个模拟下游服务，明确演示：

- `WebClient` 发请求
- 响应回来后继续拼接订单数据
- 再写入 R2DBC

## 7. 如果必须接阻塞式老组件怎么办

如果项目里短期内没法把老 JDBC / 老 SDK 全部换掉，至少要做隔离：

```java
Mono.fromCallable(blockingCall)
    .subscribeOn(Schedulers.boundedElastic())
```

这不是最佳解，但比直接在 event-loop 里执行阻塞代码安全得多。

注意：

- 这只是“隔离阻塞”，不是“把阻塞变成响应式”
- 如果阻塞操作本身很多，`boundedElastic` 也可能被拖满
- 真要长期跑高并发，还是应该把关键 IO 改成原生异步驱动

## 8. 你可以重点记住的面试表达

- WebFlux 想吃到多路复用/高并发收益，不能只改 Controller 返回类型，数据库和下游 HTTP 调用也要尽量异步化。
- 在响应式链路里混用 JDBC 或 `block()`，本质是把 event-loop 当工作线程用，容易引发吞吐下降、连接池等待放大和超时。
- PostgreSQL 主从场景下，R2DBC 可以用 failover URL 和 `targetServerType` 做基础的读写分离与故障转移。
- 读写分离不仅是连接配置问题，还涉及复制延迟导致的读写一致性取舍。

## 9. 外部参考链接

- R2DBC PostgreSQL 多 host / `targetServerType` / failover 说明：
  https://github.com/pgjdbc/r2dbc-postgresql
- R2DBC Pool 连接池与生命周期说明：
  https://github.com/r2dbc/r2dbc-pool
- Spring Data R2DBC Repository 与多 `R2dbcEntityOperations` 说明：
  https://docs.spring.io/spring-data/relational/reference/r2dbc/repositories.html
