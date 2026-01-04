# 流式导出方案说明（给技术负责人/老板）

## 1) 背景与目标

我们的典型数据规模是：**Parquet ~1GB，展开 CSV 可到十几 GB**。传统“先把行攒成对象/字符串，再写响应”的方式，在客户端慢或并发多时会堆积大量内存，很容易 OOM。  
目标是：**不落地、不攒内存、可控并发、可被背压**，同时保证 CSV/ZIP 输出可读与稳定。

---

## 2) 方案总览（核心思路）

1. **行读行写**：ParquetReader 每次读一行，立刻写一行 CSV；不构造 `Flux<Map<...>>` 这类行对象流。
2. **阻塞 IO → 响应式桥接**：使用 `DataBufferUtils.outputStreamPublisher` 把阻塞式写出逻辑桥接到 WebFlux 的响应流。
3. **背压生效**：客户端慢时，HTTP 写入会阻塞，进而让上游读 Parquet 的循环自然变慢，不会积压内存。
4. **ZIP 流式输出**：`ZipOutputStream` 支持边写边压缩，ZIP 内仅一个 CSV entry，不需要中间文件。

对应代码：

- `src/main/java/com/aquarius/wizard/webfluxparquetexportdemo/service/ParquetExportService.java`
- `src/main/java/com/aquarius/wizard/webfluxparquetexportdemo/service/ParquetGenerateService.java`

---

## 3) 已做的优化与设计点

- **避免“行对象堆积”**：取消 `Flux<Map<...>>`，直接 OutputStream 写出。
- **有界线程池**：导出逻辑在独立阻塞线程池里执行，队列满直接 503，防止阻塞任务跑到 Netty 事件循环。
- **背压链路清晰**：`response.writeWith(...)` 天生支持背压；输出慢就写慢，上游自然减速。
- **不落地 CSV/ZIP**：只在需要时临时落地 Parquet（模拟远端下载），CSV/ZIP 纯流式。
- **首包延迟与吞吐平衡**：CSV 头可立即 flush，后续按字节阈值低频 flush；ZIP 默认不频繁 flush 保持压缩效率。
- **客户端中断处理**：Broken pipe/connection reset 直接终止读取，避免无意义消耗。
- **编码策略明确**：CSV UTF-8，BINARY 字段输出 Base64，保证跨工具可读。
- **指标已接入**：导出耗时/字节数/队列深度/拒绝次数，支持容量评估与告警。

---

## 4) 实际效果（当前观测）

- 典型测试：**932MB Parquet 展开为 ~14.5GB CSV**。  
- 单次导出时内存稳定在 **~2.5GB–2.8GB** 区间波动，未出现明显的“随输出增长而线性膨胀”的现象。  
- 说明“流式读写 + 背压 + 有界线程池”策略有效地把内存占用控制在可预测范围内。

> 备注：Swagger/浏览器/Postman 对超大下载往往有上限或内部缓冲限制，出现失败更多是**客户端限制**，并非服务端 OOM。超大文件建议用 `curl`/`wget` 等命令行工具验证。

---

## 5) 为什么能扛住大文件与慢客户端

- **慢客户端**：写阻塞 -> 读阻塞，天然节流；不会在内存里堆积“行对象”或“字符串”。
- **超大文件**：内存常量级，主要由 Parquet 读缓冲 + 输出缓冲组成，不随输出大小线性增长。
- **并发压力**：有界线程池 + 队列满拒绝策略，避免过载时拖垮 Netty 线程或 JVM。

---

## 6) 风险与边界

- **超长连接**：大文件导出是长连接，需关注客户端/网关超时设置。
- **客户端限制**：某些 GUI 客户端会在内存中缓存响应，导致下载失败；建议大文件用命令行或异步导出。
- **临时磁盘空间**：模拟远端 parquet 会落地到本地临时目录（按请求清理），需保障磁盘空间。

---

## 7) 可观测性与告警

- 已通过 Actuator 暴露导出指标（`/actuator/metrics`），包含：
  - `demo.export.duration`（按 format/operation 标签）
  - `demo.export.bytes`
  - `demo.export.rejections`
  - `demo.export.executor.queue.size` / `demo.export.executor.queue.remaining`
  - `demo.export.executor.active` / `demo.export.executor.pool.size`

---

## 8) 可选的进一步优化（按需）

1. **异步导出/任务化**：将超大导出转为后台任务并写回对象存储，返回下载链接。
2. **列投影/过滤**：支持选择列、条件过滤，减少导出体积与耗时。
3. **并发与配额**：按租户/用户做并发数限制、速率限制，保护系统稳定性。
4. **更明确的硬限制**：最大输出行数/字节数、最大执行时长等策略化控制。

---

## 9) 结论

本方案本质是**架构级优化**（流式 + 背压 + 有界执行），而非细枝末节的微优化。  
它把“内存风险”从“跟输出量线性增长”降低为“与少量缓冲和 Parquet 读写开销相关的常量级增长”，  
因此在大文件、慢客户端、非高并发场景下能稳定运行并显著降低 OOM 风险。
