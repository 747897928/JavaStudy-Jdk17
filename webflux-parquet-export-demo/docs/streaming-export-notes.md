# Parquet → CSV → ZIP（WebFlux 流式导出）笔记

本文是 `webflux-parquet-export-demo` 的“实现思路 + 技术点 + 取舍”记录，目标是把你们线上类似需求（S3/GCS Parquet → 流式下载 ZIP(CSV)）整理成一份可复用的笔记。

## 0. 背景与需求（复述）

你们的真实业务场景通常是：

- Parquet 文件来源：S3 / GCS（实际解析需要 `HadoopInputFile`，因此会先落地到本地临时文件，再用 `ParquetReader<Group>` 读取）
- 文件规模跨度大：几十 KB、几 MB、几十 MB、几百 MB、2GB 甚至更大
- Parquet 压缩率高：例如 16MB Parquet 可能解压展开为 600MB+ CSV（这也是 Parquet → CSV 的典型现象）
- JVM 内存上限不高（例如 4GB），不能把 CSV/ZIP 全量攒内存，也不希望先落地 CSV 再 zip（会占用大量磁盘并慢）
- 最终交付：HTTP 下载一个 ZIP 文件（ZIP 内只有一个 CSV entry，名称为 `<baseName>.csv`）

关键风险：

- **OOM**：如果实现是“上游无限产出、下游慢则无界缓冲”，就会爆内存
- **下载体验**：小输出/小文件时，如果迟迟没有任何字节发给客户端，前端会感觉“卡住”
- **ZIP 正确性**：ZIP 的中央目录在末尾，必须写到 `finish()/close()` 才是完整 ZIP；客户端下载中断时应尽快停下

## 1. 为什么“Parquet → CSV”会很大（输入大小不可信）

Parquet 是列式存储 + 压缩 + 编码（字典、RLE、bit packing 等），尤其对于重复值/数值列会非常小。

CSV 是行式文本：每个字段都要变成字符串（例如 BINARY 输出为 Base64 文本），还要加分隔符、引号、换行。

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
ZipOutputStream(entry=<baseName>.csv)  // ZIP 模式
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

## 3. ZIP 的流式写出与 `StreamUtils.nonClosing(...)`

### 3.1 ZIP 结构与 `finish()`

ZIP 的中央目录在末尾，所以：

- `putNextEntry("<baseName>.csv")` 之后写 entry 内容，可以持续写出并持续被压缩
  - 但只有写到 `closeEntry()` + `finish()`（或 `close()`）后，ZIP 才是完整可解压文件

### 3.2 为什么要 `StreamUtils.nonClosing(...)`

在 WebFlux 里，响应体由框架控制生命周期：`outputStreamPublisher` 提供的底层 OutputStream 不希望被业务代码提前关闭。

但 `ZipOutputStream.close()` 默认会关闭底层流。

因此实现中用了 Spring 提供的：

- `StreamUtils.nonClosing(outputStream)`：返回一个“忽略 close()”的 OutputStream wrapper，不让业务代码提前关闭底层 HTTP 响应流

这样可以：

- 使用 try-with-resources 安全关闭 `ZipOutputStream`（释放 deflater 资源）
- 同时不影响 WebFlux 对响应流的管理

补充：这不等于“永远不 close”。

- `StreamUtils.nonClosing(...)` 只是不让 <u>业务代码</u> 提前关闭“HTTP 响应底层流”
- 当 `outputStreamPublisher` 的 consumer 正常结束（或异常/取消）后，Spring 会在合适的时机关闭/回收底层资源

实现位置：

- `src/main/java/.../service/ParquetExportService.java` / `src/main/java/.../service/ParquetGenerateService.java`（调用 `StreamUtils.nonClosing(...)`）

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
  - `writeZipCsvTo(...)`：StreamUtils.nonClosing + zip-level + finish

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

## 5. BINARY 输出（UTF-8 + Base64）

CSV 是文本格式；为了保证跨系统/工具可读，本项目采用约定：

- CSV 输出编码：`UTF-8`
- 对 BINARY/FIXED_LEN_BYTE_ARRAY：输出为 Base64 文本（避免任意 bytes 破坏 UTF-8 编码/CSV 可读性）
- 仍按 RFC4180 最小规则做 CSV 转义（逗号/引号/换行加引号，引号双写）

注意：

- Base64 会让输出变大，但格式更通用；如果你确实需要“原始 bytes 可逆且不 Base64”，才考虑 ISO-8859-1 这种约定编码策略

## 6. 客户端断开（Broken pipe / Connection reset）

当客户端取消下载/断开连接时：

- WebFlux 写响应会失败，最终在写入端表现为 `IOException`（broken pipe / connection reset 等）
- 正确处理：捕获后 **停止 ParquetReader 循环**，尽快退出，避免无意义继续读文件占用 IO/CPU

本项目实现中：

- `isClientAbort(IOException)` 做了常见错误消息匹配
- 捕获后直接 return

## 6.1 本地临时 Parquet 文件清理（完成/取消都删除）

真实业务里，Parquet 往往来自 S3/GCS，需要先“流式下载到本地临时文件”，再给 `ParquetReader` 用。

本示例的 `GET /demo/download` 会：

1. 模拟拿到一个“远端 InputStream”（通过先生成一个 *remote* parquet 文件，再用 InputStream 复制到 *local* parquet 文件）
2. 使用 *local* parquet 文件走 Parquet → CSV/ZIP 的导出流程
3. **当下载完成、报错、或客户端取消（cancel）时，删除这个本地临时 parquet 文件**

实现方式：

- `DemoController` 使用 `Mono.usingWhen(...)` 包住一次请求的临时文件资源
- 在 `onComplete/onError/onCancel` 都调用 `ParquetStagingService.deleteStagedParquet(...)` 做 best-effort 删除（会删除该请求的临时目录）

补充（文件名与下载名一致性）：

- 本示例的“模拟下载”会使用随机字母/数字作为 baseName（例如 `AbC123xYz890`），并将本地临时文件命名为 `AbC123xYz890.parquet`
- 因此导出响应文件名会自然符合：`AbC123xYz890.parquet` / `AbC123xYz890.csv` / `AbC123xYz890.zip`（ZIP 内 entry 为 `AbC123xYz890.csv`）

## 6.2 导出完整性与可选安全限制（拒绝而不是截断）

某些服务会加一个“最多导出 N 行”的限制来保护服务器，但如果把它做成“导出到一半就 break”，会导致：

- CSV/ZIP 文件是“语法正确但语义不完整”的截断结果
- 客户端拿到的文件很难察觉是被截断的（风险比直接报错更大）

因此本项目采用 **reject（fail-fast）** 的策略：

- 配置项：`demo.export.max-allowed-rows`（默认 `0` 表示不限制）
- 仅对 CSV/ZIP 生效（PARQUET 原样下载不需要）
- 在开始 streaming 前读取 Parquet footer 计算总行数，超过限制则直接返回 HTTP 413

对应代码位置：

- `src/main/java/com/aquarius/wizard/webfluxparquetexportdemo/service/ParquetExportService.java`：`validateBeforeStreaming(...)`
- `src/main/java/com/aquarius/wizard/webfluxparquetexportdemo/service/DemoDownloadService.java`：在设置响应头/开始 writeWith 前先做校验

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

- 生成 Parquet（直接下载，不落地）：`src/main/java/.../service/DemoGenerateService.java` + `src/main/java/.../service/ParquetGenerateService.java`
- 下载接口（模拟 S3/GCS → 本地临时 parquet → 导出）：`src/main/java/.../service/DemoDownloadService.java` + `src/main/java/.../service/ParquetStagingService.java`
- Controller（尽量薄）：`src/main/java/.../controller/DemoController.java`
- CSV 写入与转义：`src/main/java/.../util/CsvUtil.java`
- INT96：`src/main/java/.../util/Int96Util.java`
- flush/stream 工具：
  - `src/main/java/.../io/CountingOutputStream.java`
  - 备注：也可以用 Apache Commons IO / Guava 的同名实现，但本项目为减少依赖只保留了一个很小的自实现
- 参数：`src/main/java/.../config/ExportProperties.java` + `src/main/resources/application.yml`

---

## 附录 A：其他 AI 的笔记（原文摘录，未完全校验）

说明：下面内容来自“其他 AI”输出，你希望“先记下来”。其中有些表述可能偏绝对（例如“write() 在无 demand 时一定阻塞”等），请以 Spring/Reactive Streams 的官方文档与实际压测行为为准。本附录不代表本项目一定完全同意其中的所有细节，只做资料留存。

- PDF：`webflux-parquet-export-demo/webflux_parquet_csv_zip_streaming_notes.pdf`

### 原文

> # WebFlux 中将 Parquet 流式转 CSV 并实时压缩为 ZIP 的实现思路与原理说明
>
> ## 0. 背景与目标
>
> 你们的核心约束非常明确：
>
> * Parquet 可能非常大（几十 MB 到数 GB），CSV 可能膨胀很多倍。
> * 服务器 JVM 内存有限（例如 4GB），不能把 CSV/ZIP 或大量中间对象攒在内存。
> * 希望“边读边写”，下载可以慢，但必须持续输出；客户端断开后要尽快停止，不能继续读到把内存拖爆。
>
> 你们已经确认不再走 `Flux<Map<String,Object>>` 这条路，这个方向是正确的（原因见第 3、7 节）。
>
> ---
>
> ## 1. 什么是背压（Backpressure）
>
> 背压来自 Reactive Streams 规范，本质是“下游拉取（pull）驱动上游生产（push）”的**流量控制**：
>
> * **下游（Subscriber）通过 `request(n)` 表达需求（demand）**：我现在最多还能处理 n 个元素。
> * **上游（Publisher）必须遵守：发送的 `onNext` 不能超过已请求数量**，否则就意味着上游在“强推”数据、逼迫系统在中间缓存，最终容易 OOM。规范里有明确的 MUST 约束：Publisher “MUST NOT emit more onNext than requested”。
> * 规范的设计目标之一就是：不必为了匹配上下游速率而在中间**无限缓冲**，而是由 Subscriber 控制队列边界。
>
> 在 WebFlux 下载场景里，“下游”通常就是**网络写出端**：客户端带宽慢、TCP 窗口小、或者客户端暂时不读，都会让下游 demand 下降；正确的做法是上游也随之变慢，而不是继续生产并缓存。
>
> ---
>
> ## 2. 你们现状（Flux.create + Map）为什么容易 OOM
>
> 你描述的做法大概率是：
>
> * `Flux.create` + `while(reader.read() != null) sink.next(map)` 这种“上游紧密 while 循环推送”；
> * 每行 new 一个 `Map<String,Object>`，再在 reactive 链路里 `index().flatMapSequential(...)` 组装 CSV 的 `Flux<DataBuffer>`。
>
> 这里的结构性风险有两个：
>
> ### 2.1 `Flux.create` 的默认行为就是“下游跟不上就缓存”
>
> Reactor 在 `Flux.create` 的 Javadoc 里说得很直白：它适合你“不用担心取消和背压”，因为当下游跟不上时会**buffer all signals**（缓冲所有信号）。
> 而 `FluxSink.OverflowStrategy.BUFFER` 也明确警告：这是**无界缓冲**，可能导致 `OutOfMemoryError`。
>
> 换句话说：当网络慢（下游慢）时，你的 `while` 仍可能快速 `next(map)`，这些 map 会被队列堆住，内存必炸。
>
> ### 2.2 “每行一个 Map”会极大放大 GC 与对象滞留
>
> 即使不 OOM，也会出现：
>
> * 海量对象分配（Map、Entry、装箱类型、String 拼接/格式化）；
> * 下游慢时对象存活时间变长（因为排队等待写出），GC 压力陡增；
> * 一旦链路里某处有预取/缓存（很多 Reactor operator 有 prefetch 语义），对象滞留会进一步放大。
>
> 因此“不要用 `Flux<Map<...>>`”不仅是风格问题，而是**内存安全性**问题。
>
> ---
>
> ## 3. 关键方案：把“背压闸门”放在 OutputStream 上
>
> 你之前提到 Spring 6.1+ 的 `DataBufferUtils.outputStreamPublisher`，它正是为这种“阻塞写入逻辑 + 响应式下游”桥接准备的。
>
> 它的 Javadoc 给了你想要的全部语义保证：
>
> * `OutputStream.write()` 写入的数据会先缓冲，超过默认 chunk（1024）或 `flush()` 时，才产出一个 `DataBuffer`；
> * **如果没有 demand，`OutputStream.write()` 会阻塞直到有 demand**；
> * **如果订阅被 cancel（例如客户端断开），后续 `write()` 会抛 `IOException`**。
>
> 这意味着你可以用一个“传统的、同步的、边读边写”的导出循环来写 CSV/ZIP，但它的写出节奏会被下游 demand 自动节流——这就是背压在工程上的落点。
>
> ---
>
> ## 4. 为什么可以不落地 CSV，直接边写边压缩成 ZIP
>
> ### 4.1 ZIP 写入天然支持流式
>
> `ZipOutputStream` 的语义就是“顺序写 entry”：
>
> * `putNextEntry`：开始一个新的 ZIP entry，把流定位到 entry 数据起点；
> * 你可以不断 `write(byte[], off, len)` 把 CSV 字节写进去；
> * `closeEntry`：关闭当前 entry，准备下一个；
> * `finish`：写完 ZIP 的中央目录等收尾结构（不关闭底层流）。
>
> 重要点：**写 ZIP entry 并不要求你预先知道 CSV 完整大小**。因此你完全可以：
>
> > ParquetReader 读一行 → 格式化成 CSV 一行 → 写入 ZipOutputStream →（由 outputStreamPublisher 转成 DataBuffer）→ 写到网络
>
> ### 4.2 “解压后必须是原 CSV”如何保证
>
> 只要你写入 ZIP entry 的字节序列就是你想要的 CSV（比如 UTF-8 + `\n`），ZIP 的压缩（DEFLATE）是**无损**的；解压得到的字节序列与写入前一致。你不需要先落地 CSV 才能保证一致性，一致性来自“你写进去的就是最终 CSV 字节”。
>
> 工程上你需要做的只是：
>
> * 明确 CSV 编码（建议 UTF-8）；
> * 严格实现 CSV 转义（逗号、双引号、换行等）。
>
> ---
>
> ## 5. 为什么“读一行写一行 + outputStreamPublisher”就不会 OOM
>
> 把内存占用拆开看，你就能直观看到它为什么稳：
>
> ### 5.1 你不再创建“可堆积的行对象流”
>
> 旧方案：行对象（Map）→ reactive 队列/预取 → 网络慢就堆积。
> 新方案：行对象只在**当前循环迭代**存在，写完即丢；不会形成“可无限堆积的中间集合”。
>
> ### 5.2 下游慢时，上游会被迫慢下来（关键在“write 阻塞”）
>
> 当客户端慢导致 demand 降低时：
>
> * `outputStreamPublisher` 让 `OutputStream.write()` 阻塞（无 demand 不让你继续写）。
> * 你的导出线程卡在写出点，自然不会继续从 ParquetReader 往下读，从而不会继续产生更多行数据、更多对象。
> * 这正符合 Reactive Streams 的核心约束：生产速率受下游需求控制，避免无界缓冲。
>
> ### 5.3 客户端断开后不会继续读到死
>
> 客户端断开会触发 cancel：
>
> * `outputStreamPublisher` 规定：cancel 后 `write()` 会抛 `IOException`。
> * 你在导出循环里捕获这个异常并退出，同时 `close()` ParquetReader/ZipOutputStream/文件句柄，即可“断即止损”。
>
> ---
>
> ## 6. DataBufferUtils.outputStreamPublisher 在底层到底做了什么
>
> 你可以把它理解成一个“把阻塞 OutputStream 写入变成 `Publisher<DataBuffer>`”的适配器：
>
> * 你提供一个 `Consumer<OutputStream>`，里面写你熟悉的阻塞 I/O 逻辑（读 Parquet、写 CSV/ZIP）。
> * Spring 在内部把你写入的字节按照 chunk 切片包装成 `DataBuffer`（由 `DataBufferFactory` 创建）。
> * WebFlux 服务器（Reactor Netty 等）订阅这个 Publisher，把 `DataBuffer` 写到网络。
>
> 并且 Spring 的 DataBuffer 抽象本身就是为网络 I/O 做的：`DataBufferFactory` 可以分配或包装缓冲区；底层实现可能是 Netty 的池化 ByteBuf，对性能/拷贝有帮助。
>
> ---
>
> ## 7. 为什么我们明确不再用 Flux<Map<...>>（给你写笔记用的结论）
>
> 一句话总结：**Flux<Map> 在“上游可能很快、下游可能很慢”的导出链路中，会把速率不匹配转化为“对象堆积”，而对象堆积就是 OOM 的前兆。**
>
> 更具体地、可以直接抄到笔记里：
>
> 1. `Flux.create` 在下游跟不上时会缓冲所有信号（不担心背压），非常容易无界堆积。
> 2. Map/装箱/字符串等对象分配巨大，且在回压下存活时间变长，GC 压力飙升。
> 3. reactive 操作符普遍存在预取/排队语义；你很难从整体上证明“任何时候都不会缓存太多行对象”。
> 4. 用 `outputStreamPublisher` 把背压闸门放在“最靠近网络”的位置，才能让上游读取天然与网络消费对齐。
>
> ---
>
> ## 8. 这个方案的收益与适用场景
>
> ### 能应对的场景
>
> * Parquet 几百 MB/数 GB、CSV 膨胀很大但允许慢慢下；
> * JVM 内存不大、但磁盘和网络可用；
> * 客户端可能中途取消，需要服务端尽快停止（不做无意义读/算）。
>
> ### 主要收益
>
> * **内存上界清晰**：单行对象 + 少量缓冲（writer buffer、zip deflater buffer、chunk buffer）。
> * **背压天然生效**：网络慢 → demand 小 → write 阻塞 → 上游读 Parquet 自动慢。
> * **无需落地 CSV**：减少磁盘占用、减少一次额外 I/O；同时避免“落地 CSV 后再压缩”的二次遍历。
>
> ---
>
> ## 9. 资源释放/泄露自查清单（落地到本项目）
>
> 这部分是对当前实现的一次“资源/内存”审计结论，方便你后续迁移到真实业务（S3/GCS → 本地临时 parquet → 导出）时做 checklist。
>
> ### 9.1 Parquet/Hadoop 相关资源必须 close
>
> - 读 schema：`ParquetFileReader` 必须 close（本项目 `readSchema(...)` 已使用 try-with-resources）
> - 读数据：`ParquetReader<Group>` 必须 close（本项目 `writeCsvTo(...)` 已使用 try-with-resources）
>
> 这两类如果忘记 close，最常见的后果是“文件句柄泄露”，表现为导出次数多了以后打不开新文件。
>
> ### 9.2 ZIP 相关资源必须 finish/close
>
> - 顺序：`putNextEntry(...)` → 持续写 entry 内容 → `closeEntry()` → `finish()`（本项目已按此实现）
> - `finish()` 会写 ZIP 中央目录（没有它 ZIP 可能不完整不可解压）
>
> ### 9.3 为什么要 `StreamUtils.nonClosing(...)`
>
> - `ZipOutputStream.close()` 默认会关闭底层流
> - WebFlux 响应底层流由 Spring 管理，不希望业务代码提前 close
> - 本项目用 `StreamUtils.nonClosing(...)` 防止业务代码提前关闭底层 HTTP 响应流：让 ZIP wrapper 能正常 close/释放资源，但不会把响应流提前关掉
>
> ### 9.4 客户端取消/断开时是否会“继续读 parquet 做无用功”
>
> - 取消后写端通常会出现 `IOException`（broken pipe / connection reset 等）
> - 本项目在 CSV/ZIP 写循环里捕获这类异常后会停止读取，从而尽快释放 ParquetReader/ZipOutputStream
>
> ### 9.5 如果混用其他 DataBuffer API，需要注意 DataBuffer release
>
> 本项目的导出链路是“从 OutputStream 产出 DataBuffer”，通常不需要手动 release。
>
> 但如果你在其他地方使用 `DataBufferUtils.write(...)`、手工拼 `DataBuffer` 并缓存/转发，需要特别注意：
> - 某些 API 不会自动 release 上游 `DataBuffer`，需要显式 `DataBufferUtils.releaseConsumer()` 或者确保下游会释放
>
> ---
>
> ## 10. 线程池/调度的两个改进点（已落地）
>
> ### 10.1 复用 `Scheduler`，避免每次请求 new wrapper
>
> - 旧写法：每次调用 `Schedulers.fromExecutor(exportExecutor)` 都会创建一个新的 `Scheduler` 包装对象
> - 新写法：在 Spring 容器里提供单例 `exportScheduler`，业务代码复用这个 Scheduler（避免重复创建/潜在 GC 压力）
>
> ### 10.2 Executor 饱和策略：拒绝 > CallerRuns
>
> - `CallerRunsPolicy` 的风险：线程池满时会在“提交任务的线程”执行阻塞导出逻辑；如果提交线程是 Netty event-loop，会直接把 WebFlux 服务器卡死
> - 本项目改为 `AbortPolicy`：当线程池队列满时直接拒绝，让请求快速失败（一般返回 503），客户端可重试
> - 同时提供了 `assertExportCapacityOrThrow()` 在开始 streaming 前尽量 fail-fast，减少“写到一半才失败”的概率
>
> ---
>
> ## 11. 对“外部审计建议”的核对结论（批判性记录）
>
> 下面是对某些常见建议的核对结论（大多数是对的，但需要理解边界）：
>
> - “`Flux.create` 默认 BUFFER 可能 OOM”：结论成立；它更像 push 模式，若生产速度 > 消费速度，会无界堆积信号/对象。
> - “`outputStreamPublisher` 能把背压传导到阻塞写循环”：结论基本成立；下游慢时会让写端变慢（阻塞/等待），从而限制上游读取速度，内存更可控。
>   - 边界：网络栈/框架仍可能有少量缓冲（例如 Netty/TCP），所以它不是“0 缓冲”，但关键是“不会无界增长”。
> - “取消后 `write()` 抛 `IOException`，要立即停止读取”：结论成立；本项目按此处理（`isClientAbort(...)`）。
> - “`DataBufferUtils.join/collectList` 会把内容攒内存”：结论成立；不适用于大文件导出。
> - “`DataBufferUtils.write(...)` 可能需要手动 release”：这条是对的，但与本项目主链路无关（本项目从 OutputStream 产出 DataBuffer，通常不需要你手工 release）。
>
> ---
>
> ## 12. 你做 PoC/压测时建议重点观察的指标
>
> 1. **堆内存曲线**：导出过程中是否平稳（应接近平台化，而不是随时间增长）。
> 2. **GC 次数/停顿**：Map 方案会明显更差；新方案应显著下降。
> 3. **取消行为**：客户端下载中断后，服务端是否在秒级停止读 parquet，并释放文件句柄。
> 4. **吞吐**：CPU（压缩）、磁盘（读 parquet）、网络（写 zip）谁是瓶颈。
>
> ---
>
> ## 参考依据（对应你要的“技术原理出处”）
>
> * Reactive Streams 规范：Publisher 不得发送超过请求量的 onNext（背压核心约束）。
> * Reactive Streams 规范：由 Subscriber 控制队列边界、避免无限缓冲。
> * Reactor `Flux.create`：下游跟不上时通过缓冲处理（buffer all signals）。
> * Reactor `FluxSink.OverflowStrategy.BUFFER`：无界缓冲可能导致 OOM。
> * Spring `DataBufferUtils.outputStreamPublisher`：无 demand 时 `write()` 阻塞；cancel 后 `write()` 抛 IOException；chunk/flush 行为。
> * Java `ZipOutputStream`：putNextEntry/closeEntry/finish 的顺序写入语义，支持流式产出 ZIP。
> * Spring DataBuffer 抽象：DataBufferFactory、池化/包装等能力（理解 WebFlux 写出模型有帮助）。
