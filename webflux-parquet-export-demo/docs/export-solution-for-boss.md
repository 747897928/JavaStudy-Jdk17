# Parquet 导出方案说明（给老板/评审用）

> 目标：解释我们如何在 **内存有限（例如 JVM 4GB）** 的情况下，把 **Parquet** 导出为 **Parquet/CSV/ZIP(CSV)**，并且支持大文件下载；同时解释为什么这个实现不容易 OOM，以及在超大 CSV/ZIP 场景下客户端为什么会 OOM/失败。

## 1. 背景与约束

我们的业务输入是 Parquet（来自 S3/GCS 等对象存储）。文件规模跨度很大：几十 KB、几十 MB、几百 MB、甚至 GB 级。

关键事实：

- **Parquet 很“省空间”**：列式 + 编码 + 压缩，尤其重复值多/数值列多时压缩率更高。
- **CSV 很“膨胀”**：行式文本，每个字段都要转成字符串并加分隔符、引号、换行等；因此经常出现：  
  `932MB Parquet → 14.5GB CSV`（这是合理且常见的膨胀）。

我们的服务端约束：

- JVM 内存有限（例如 4GB），不能把整份 CSV/ZIP 或大量行对象放进内存。
- 目标是 **“边读边写”**：客户端下载慢没关系，但必须能持续下载，不允许把所有数据先攒内存再吐出。
- Parquet 解析通常需要 **可 seek/随机读** 能力（要读 footer/metadata）；真实业务里我们会把 S3/GCS 的对象 **先落盘为本地临时 Parquet 文件** 再解析。

## 2. 方案概览（核心链路）

我们支持三种导出：

- `PARQUET`：原样把 parquet 文件输出给客户端
- `CSV`：Parquet → CSV（不落地 CSV、不攒内存）
- `ZIP`：Parquet → CSV → ZIP（不落地 CSV/ZIP、不攒内存）

核心链路（CSV/ZIP）都是同一个思想：**读一行写一行**：

1. `ParquetReader<Group>` 读取一条记录（row group 中的一行）
2. 立即把这行转换成 CSV 一行并 `write(...)` 到输出流
3. ZIP 模式则把 CSV 字节写入 `ZipOutputStream` 的一个 entry 里（边写边压缩）
4. 输出流的“落点”不是文件，而是 **HTTP 响应流**（流式下发）

## 3. 为什么这个方案不容易 OOM（关键技术点）

### 3.1 为什么不用 `Flux<Map<...>>` / “每行一个 Map”

直观但危险的写法是：

- 读 Parquet：while 循环很快
- 每行 `new Map` 或 `new String`
- 交给 reactive pipeline 输出

风险是：**下游慢（网络慢）时，上游仍然快**，中间会产生“无界缓冲”，大量行对象堆积，最终 OOM（还会触发严重 GC）。

### 3.2 我们使用 `DataBufferUtils.outputStreamPublisher` 做“背压闸门”

我们采用 Spring WebFlux（Reactor）并使用 Spring 6.1+ 的：

- `DataBufferUtils.outputStreamPublisher(consumer, bufferFactory, executor, chunkSize)`

它的作用是：

- Spring 给我们一个 `OutputStream`
- 我们在 `consumer` 里做阻塞式导出逻辑（ParquetReader/ZipOutputStream）
- Spring 把我们写入的字节按 **chunkSize** 组装成 `DataBuffer` 并流式写入 HTTP 响应

关键点：**背压（Backpressure）会从网络写出端传导回来**。

当客户端很慢时：

- WebFlux/Netty 写网络会变慢
- `outputStreamPublisher` 的 write/flush 发布节奏会被下游消费速度限制
- 结果是：我们的导出线程会自然“写不动”，从而不会继续疯狂读取 Parquet 并制造更多中间对象

这使得内存占用更接近“固定上限”，而不是随文件大小增长。

### 3.3 阻塞 IO 不占用 Netty event-loop（避免拖死整个服务）

ParquetReader/ZipOutputStream 属于阻塞 IO + CPU（压缩）。

我们把这些工作放到独立的 **有界线程池**（`exportExecutor`）上执行，而不是 Netty event-loop：

- 避免一个大导出把整个 WebFlux 服务器“卡死”
- 队列是有界的，线程池饱和时会拒绝并返回 503（显式失败，客户端可重试），避免“请求无限堆积”造成系统级压力

### 3.4 “一次写多少字节、多久 flush 一次”（可控且简单）

我们的输出是“多层 buffer”叠加后的结果：

- `outputStreamPublisher` 的 `chunkSize`：决定一次向 WebFlux 下发的 `DataBuffer` 粒度  
  - 当前 demo 默认 `chunkSize=64KB`
- `BufferedOutputStream`：减少许多小写（逗号/引号/换行）带来的系统调用开销  
  - 当前 demo 默认 `outputBufferSize=64KB`
- CSV flush 策略（体验优化，不影响正确性）：
  - 写完 header 可选择 flush（降低小文件首包延迟）
  - 之后按“累计输出字节阈值”低频 flush（例如 1MB）
- ZIP flush 策略更保守：
  - 频繁 flush 可能影响 deflate 压缩效率；通常只在结束时 `finish()/flush()` 即可

这些参数的目标是：

- 对小文件：尽快下发首字节，用户感知“下载开始了”
- 对大文件：尽量避免每行 flush 的性能灾难，同时持续、平稳地输出

### 3.5 取消下载/断线处理（不做无用功）

客户端取消或断开连接后，服务端写响应会失败（常见表现为 `IOException` 或 Reactor Netty 的 `AbortedException`）。

我们做了两件事：

- 识别“客户端断线”类异常（`AbortedException` / `ClosedChannelException` / 常见 reset/broken pipe 等）并安静退出，减少错误日志噪音
- 一旦识别为 abort，就停止读取 Parquet（避免继续消耗 CPU/IO 做无意义工作）

## 4. ZIP 为什么可以边写边压缩（不需要先落地 CSV）

ZIP 写入天然是流式的：

1. `putNextEntry(<baseName>.csv)`
2. 不断 `write(...)` 把 CSV 字节写进 entry
3. `closeEntry()` + `finish()` 写完中央目录（ZIP 文件尾部结构）

因此 ZIP 不需要提前知道 CSV 的总大小，也不需要把 CSV 先落到磁盘再压缩。

## 5. 为什么 Swagger/浏览器/Postman 在超大 ZIP 场景会 OOM/失败

这部分是“客户端行为”导致的，不是后端没 close。

典型情况（非常常见）：

- Swagger UI / 浏览器 JS 代码为了下载二进制，会用 `fetch(...).blob()` 或 `arrayBuffer()`  
  **这会把整个响应体读到内存里**，当 ZIP 达到 GB 级时，浏览器必然 OOM。
- Postman 也常把响应体缓存在内存用于展示/预览，GB 级响应同样容易失败。

为什么 CSV 在 Swagger 看起来“能下”？

- 有些情况下 CSV 会走浏览器原生下载路径（更像“直接落盘”），不一定会把全部内容塞进 JS 内存；而 ZIP 更容易触发 blob/arrayBuffer 缓冲。

正确验证方式：

- 用 `curl -L -o out.zip <url>` 或浏览器“直接打开下载链接并保存”（不走 JS blob）来验证服务端是否能稳定输出。

## 6. 现实落地还可能需要补齐的需求清单（建议老板确认）

为了避免后续返工，建议尽早确认这些“业务/产品”需求：

- **进度展示**：是否需要准确进度条？  
  - 流式 CSV/ZIP 我们通常无法提前给 `Content-Length`（不做两遍转换就无法知道最终大小），前端只能显示“已下载字节数”而不是百分比。
- **断点续传/Range**：是否要求支持断点续传？  
  - 对“即时生成”的 CSV/ZIP 很难做 Range；除非先生成并存储成文件（这会回到落地与磁盘成本）。
- **并发与限流**：是否需要每租户/每用户限流、并发数上限？  
  - 导出属于重 IO/CPU 任务，通常需要限流与排队策略。
- **临时文件容量与清理**（S3/GCS 落盘场景）：  
  - 临时目录空间、超时清理策略、异常退出时的兜底清理、磁盘告警
- **字段映射/格式规范**：  
  - CSV 分隔符、换行符、是否需要 header、时间戳格式、BINARY 输出策略（当前 demo：UTF-8 + Base64，保证 CSV 永远合法文本）
- **超时/网关限制**：  
  - 长连接下载可能受到网关 idle timeout 影响，必要时要配合网关参数与更保守的 flush 策略。

## 7. 本项目实现对应的代码位置（方便追溯）

- 导出核心（Parquet → CSV/ZIP 流式）：`src/main/java/.../service/ParquetExportService.java`
- 模拟 S3/GCS → 本地临时 parquet + 自动清理：`src/main/java/.../service/ParquetStagingService.java`
- 下载接口编排：`src/main/java/.../service/DemoDownloadService.java`
- 生成 parquet（不落地，直接下发）：`src/main/java/.../service/ParquetGenerateService.java`
- Parquet 类型转 CSV 文本（含 LogicalType/ConvertedType 兼容）：`src/main/java/.../parquet/ParquetToCsvCellValueConverter.java`

