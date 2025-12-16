# Parquet → CSV → ZIP（WebFlux 流式导出）笔记

本文是 `webflux-parquet-export-demo` 的“实现思路 + 技术点 + 取舍”记录，目标是把你们线上类似需求（S3/GCS Parquet → 流式下载 ZIP(CSV)）整理成一份可复用的笔记。

## 0. 背景与需求（复述）

你们的真实业务场景通常是：

- Parquet 文件来源：S3 / GCS（实际解析需要 `HadoopInputFile`，因此会先落地到本地临时文件，再用 `ParquetReader<Group>` 读取）
- 文件规模跨度大：几十 KB、几 MB、几十 MB、几百 MB、2GB 甚至更大
- Parquet 压缩率高：例如 16MB Parquet 可能解压展开为 600MB+ CSV（这也是 Parquet → CSV 的典型现象）
- JVM 内存上限不高（例如 4GB），不能把 CSV/ZIP 全量攒内存，也不希望先落地 CSV 再 zip（会占用大量磁盘并慢）
- 最终交付：HTTP 下载一个 ZIP 文件（ZIP 内只有 `data.csv`）

关键风险：

- **OOM**：如果实现是“上游无限产出、下游慢则无界缓冲”，就会爆内存
- **下载体验**：小输出/小文件时，如果迟迟没有任何字节发给客户端，前端会感觉“卡住”
- **ZIP 正确性**：ZIP 的中央目录在末尾，必须写到 `finish()/close()` 才是完整 ZIP；客户端下载中断时应尽快停下

## 1. 为什么“Parquet → CSV”会很大（输入大小不可信）

Parquet 是列式存储 + 压缩 + 编码（字典、RLE、bit packing 等），尤其对于重复值/数值列会非常小。

CSV 是行式文本：每个字段都要变成字符串（或者按你们的 “BINARY 原始 bytes” 策略输出），还要加分隔符、引号、换行。

因此：

- 不能用 “Parquet 文件大小” 去推断 CSV 输出大小
- 也不建议按输入大小动态调整 chunk/flush；更稳的是按“输出写入行为”来做策略

## 2. 总体方案（推荐，Spring 6.1+）

### 2.1 总体数据流（核心思路）

核心是做到“边读边写边压缩边下发”，全程不落地 CSV/ZIP、不攒内存：

```
ParquetReader<Group>
   ↓  (read one row)
Group → (format one row) → CSV bytes
   ↓  (write)
ZipOutputStream(entry=data.csv)  // ZIP 模式
   ↓  (write to response OutputStream)
DataBufferUtils.outputStreamPublisher(...)
   ↓  (Flux<DataBuffer>)
WebFlux response.writeWith(...)
   ↓
client download
```

### 2.2 为什么不用 `Flux<Map<...>>`

“每行 new 一个 Map”再组装成 `Flux<Map>`/`Flux<String>` 的写法，很容易出现：

- 上游 push 太快（读 Parquet 很快）
- 下游慢（客户端/网络慢，或者网关/代理限速）
- 结果：大量行对象（Map/StringBuilder/byte[]）堆在 JVM 堆里 → OOM

本项目的实现避免了“行对象流”，改成阻塞式“读一行写一行”，并把阻塞写桥接成响应式。

### 2.3 Spring 6.1+ 的关键：`DataBufferUtils.outputStreamPublisher`

Spring Framework 6.1+ 提供：

- `DataBufferUtils.outputStreamPublisher(consumer, bufferFactory, executor, chunkSize)`

它做的事情可以理解为：

- Spring 给你一个 `OutputStream`
- 你在 `consumer.accept(outputStream)` 里执行阻塞式逻辑（读 Parquet / 写 CSV / 写 Zip）
- Spring 把你写进去的字节按 `chunkSize` 聚合成 `DataBuffer`，以 `Publisher<DataBuffer>` 形式输出
- WebFlux 的响应写出消费这个 `Publisher<DataBuffer>`

背压的直觉：

- 客户端慢 → WebFlux 下游消费慢 → 上游发布/写出受限 → 写入端在某些时刻会“变慢/等待”
- 你不会无限堆积 DataBuffer/行对象

关键点：**内存上限主要由几个固定 buffer 决定（chunkSize、Zip deflate 内部 buffer、BufferedOutputStream 等），而不是由文件总大小决定**。

## 3. ZIP 的流式写出与 `NonClosingOutputStream`

### 3.1 ZIP 结构与 `finish()`

ZIP 的中央目录在末尾，所以：

- `putNextEntry("data.csv")` 之后写 entry 内容，可以持续写出并持续被压缩
- 但只有写到 `closeEntry()` + `finish()`（或 `close()`）后，ZIP 才是完整可解压文件

### 3.2 为什么要 `NonClosingOutputStream`

在 WebFlux 里，响应体由框架控制生命周期：`outputStreamPublisher` 提供的底层 OutputStream 不希望被业务代码提前关闭。

但 `ZipOutputStream.close()` 默认会关闭底层流。

因此实现中用了：

- `NonClosingOutputStream`：拦截 `close()`，只做 `flush()`，不关闭底层流

这样可以：

- 使用 try-with-resources 安全关闭 `ZipOutputStream`（释放 deflater 资源）
- 同时不影响 WebFlux 对响应流的管理

代码位置：

- `src/main/java/com/aquarius/wizard/webfluxparquetexportdemo/util/NonClosingOutputStream.java`

## 4. “flush 策略”：覆盖小文件 + 大文件

### 4.1 flush 的真实作用

flush 不改变最终内容，只影响“已写出的字节什么时候被推出去”：

- 没 flush：字节可能停留在某一层 buffer（BufferedOutputStream、ZipOutputStream、outputStreamPublisher 的 chunk 聚合）里
- flush：尽快把已有字节推向下游（网络）

flush 主要解决的是：

- **首包延迟**（小输出可能不满一个 chunk，如果没有 flush，客户端可能直到结束才收到首字节）
- **持续进度感**（大输出持续写本来就会不断满 chunk；是否 flush 影响通常较小）

flush 不是用来防 OOM 的；防 OOM 的关键仍是“流式 + 有界缓冲 + 背压”。

### 4.2 最终选型（本项目落地）

我们采用的策略是：

- **CSV**
  - 写完 header 立即 flush 一次（降低首包延迟，小输出更友好）
  - 后续按“写出字节阈值”低频 flush（默认 1MB），避免每行 flush 的性能灾难
- **ZIP**
  - 默认不做周期性 flush（`zipFlushEveryBytes=0`），避免影响 deflate 压缩效率
  - header flush 默认关闭（可配置开启）
  - 最后 `finish()` + `flush()` 保证 ZIP 完整

对应配置：

- `src/main/resources/application.yml`
  - `demo.export.csv-flush-header`
  - `demo.export.csv-flush-every-bytes`
  - `demo.export.zip-flush-header`
  - `demo.export.zip-flush-every-bytes`

实现位置：

- `src/main/java/com/aquarius/wizard/webfluxparquetexportdemo/service/ParquetExportService.java`
  - `writeCsvTo(...)`：header flush + bytes threshold flush
  - `writeZipCsvTo(...)`：NonClosingOutputStream + zip-level + finish

### 4.3 为什么优先“按字节阈值”而不是“按行数阈值”

同样是 10,000 行：

- 可能是 10,000 行 * 50 bytes（很小）
- 也可能是 10,000 行 * 10 KB（很大，尤其含 BINARY 或很多列）

因此按行数 flush 的效果不稳定；按字节阈值更贴近“输出体积/进度”的目标。

### 4.4 为什么 ZIP 不建议按时间周期 flush

对纯 CSV，按时间 flush 更多是体验/进度问题；但对 ZIP：

- `ZipOutputStream` 依赖 deflate
- 频繁 `flush()` 可能触发 deflater 的同步 flush 行为（实现相关），有概率降低压缩率/增加 CPU

因此 ZIP 默认不做周期性 flush，除非你们线上遇到“中间长时间无输出导致网关断开”的特殊情况，此时才考虑 **很大的字节阈值**（例如 8MB/16MB），并压测 CPU/压缩率。

## 5. BINARY “原始 bytes” 输出（ISO-8859-1）

CSV 是文本格式；但你们的需求是不做 Base64，而要尽量“原始 bytes 可逆”。

本项目采用约定：

- CSV 输出编码：`ISO-8859-1`
- 对 BINARY/FIXED_LEN_BYTE_ARRAY：直接把 `byte[]` 写入输出（0..255 一一映射到字节序列）
- 仍按 RFC4180 最小规则做 CSV 转义（逗号/引号/换行加引号，引号双写）

注意：

- 这要求消费方也用一致的编码与解析方式，否则“看起来像乱码”是预期现象
- 若跨系统/工具通用性优先，仍建议改 Base64（但不符合你们当前需求）

## 6. 客户端断开（Broken pipe / Connection reset）

当客户端取消下载/断开连接时：

- WebFlux 写响应会失败，最终在写入端表现为 `IOException`（broken pipe / connection reset 等）
- 正确处理：捕获后 **停止 ParquetReader 循环**，尽快退出，避免无意义继续读文件占用 IO/CPU

本项目实现中：

- `isClientAbort(IOException)` 做了常见错误消息匹配
- 捕获后直接 return

## 7. 线程模型：不要堵 Netty event-loop

ParquetReader、ZipOutputStream 都是阻塞 IO/CPU 操作，因此必须在专用线程池执行。

本项目通过：

- `DataBufferUtils.outputStreamPublisher(..., executor, ...)`

把 consumer 放到 `exportExecutor` 上执行；线程池是固定大小 + 有界队列，避免并发导出请求无限堆积导致内存/负载飙升。

## 8. 参数建议（经验默认值，不是定理）

下面是适合“下载场景”的常见区间：

- `chunkSize`：32KB ~ 128KB（本项目默认 64KB，偏吞吐）
- `csvFlushEveryBytes`：1MB ~ 4MB（越大越省 flush 开销，但首包/进度感弱一些）
- `zipFlushEveryBytes`：0 或 8MB+（默认 0）
- `zipLevel`：通常选 1（BEST_SPEED），除非对压缩率极其敏感

调参原则：

- 先保证不 OOM（流式 + 不攒对象 + 有界缓冲）
- 再看吞吐（CPU、磁盘、网络）
- 最后再调体验（首包、进度）

## 9. Spring 版本较老（< 6.1）的 fallback 方案（记录）

如果你在 Spring Boot 2.7 / Spring 5.3（或任何没有 `outputStreamPublisher` 的版本）仍要实现“阻塞写入 → Flux<DataBuffer>”：

可选 fallback：

1) **PipedInputStream/PipedOutputStream**
   - 生产者线程：`ZipOutputStream` 写到 `PipedOutputStream`
   - 消费者：WebFlux 使用 `DataBufferUtils.readInputStream(() -> pipedInputStream, ...)` 转成 `Flux<DataBuffer>` 写回响应
   - 缺点：更容易写出死锁/阻塞、资源关闭顺序更敏感，需要更严谨的取消/异常处理

2) **自定义有界队列（更可控，但更复杂）**
   - 生产者把 byte[] chunk 写入一个有界队列
   - 消费者把队列变成 Flux，并在 request-n 驱动下拉取
   - 优点：更容易精确控制缓冲与取消
   - 缺点：实现工作量更大，容易漏掉 corner cases（取消、异常、背压传播）

因此，如果条件允许，优先升级到 Spring 6.1+ 使用 `outputStreamPublisher`，实现更短、更安全。

## 10. 本项目当前实现位置索引

- 生成 Parquet：`src/main/java/.../service/ParquetExportService.java` → `generateDemoParquet(...)`
- 下载接口：`src/main/java/.../controller/DemoController.java`
- CSV 写入与转义：`src/main/java/.../util/CsvUtil.java`
- INT96：`src/main/java/.../util/Int96Util.java`
- flush/stream 工具：
  - `src/main/java/.../util/CountingOutputStream.java`
  - `src/main/java/.../util/NonClosingOutputStream.java`
- 参数：`src/main/java/.../config/ExportProperties.java` + `src/main/resources/application.yml`

