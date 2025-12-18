# webflux-parquet-export-demo

一个可直接运行的最小示例（JDK 17 + Spring Boot 3.5.6 + WebFlux + Parquet 1.16.0 + Hadoop 3.4.1），演示：

- 生成 Parquet 并直接下载（不落地到本地文件）
- Parquet → CSV：读一行写一行（不落地 CSV，不攒内存）
- Parquet → ZIP(CSV)：边生成 CSV 边压缩写出（不落地 CSV/ZIP，不攒内存）

## 1. 运行

在父工程目录执行：

```bash
# 确保使用 JDK17（示例：macOS）
# export JAVA_HOME=$(/usr/libexec/java_home -v 17)
# export PATH="$JAVA_HOME/bin:$PATH"

mvn -pl webflux-parquet-export-demo -DskipTests package
mvn -pl webflux-parquet-export-demo spring-boot:run
```

服务启动后默认端口 `8080`（见 `src/main/resources/application.yml`）。

说明：
- 运行前建议先确认 `mvn -v` 输出的 `Java version` 是 17（否则会出现 `UnsupportedClassVersionError`）。
- 该模块为了避免父工程里“锁死的旧依赖版本（如 Jackson 2.11、SLF4J 1.7）”与 Spring Boot 3.5.6 冲突，模块自身使用 `spring-boot-starter-parent:3.5.6` 作为 parent。
- 你本机 Maven 版本较旧（3.3.9），模块在 `pom.xml` 里显式锁定了 `maven-compiler-plugin` 等插件版本以保证可以构建运行。

## 2. 生成 Parquet（直接下载）

```bash
curl -L -OJ -X POST "http://localhost:8080/demo/generate?rows=800000"
```

该接口会直接返回一个 Parquet 下载响应（文件名为随机字母/数字，例如 `AbC123xYz890.parquet`），不在本地落地生成的 Parquet 文件。

Schema 包含以下 primitive（全部 optional，会随机缺失用于测试 `null -> empty`）：

- `INT32`/`INT64`
- `INT96`（示例转成 ISO-8601 输出到 CSV）
- `FLOAT`/`DOUBLE`
- `BOOLEAN`
- `BINARY`（写入 `byte[]`；示例生成的是可见 ASCII bytes，便于肉眼验证）

## 3. 下载导出

### 3.1 parquet 原样下载（模拟从 S3/GCS 落地到临时文件）

```bash
curl -L -OJ "http://localhost:8080/demo/download?format=parquet&source=s3a://bucket/path/file.parquet"
```

### 3.2 流式 CSV 下载（不落地、不攒内存）

```bash
curl -L -OJ "http://localhost:8080/demo/download?format=csv&source=gs://bucket/path/file.parquet"
```

### 3.3 流式 ZIP(CSV) 下载（不落地、不攒内存）

```bash
curl -L -OJ "http://localhost:8080/demo/download?format=zip&source=s3a://bucket/path/file.parquet"
# 文件名来自响应头 Content-Disposition（示例：AbC123xYz890.zip）
unzip -l AbC123xYz890.zip
# ZIP 里只有一个 CSV entry，名称与 parquet 原文件 baseName 一致（示例：AbC123xYz890.csv）
unzip -p AbC123xYz890.zip AbC123xYz890.csv | head
```

ZIP 内只有一个 entry：`<baseName>.csv`（baseName 来自 parquet 原文件名去掉 `.parquet`）。

## 4. 为什么不用 Flux<Map<...>>

“每行一个 Map”的常见写法会构造类似 `Flux<Map<String,Object>>` 的对象流，再在 reactive pipeline 里转换成 `DataBuffer`。

当客户端下载很慢时，如果上游持续 push（生产速度 > 消费速度），这些 Map/行字符串很容易在内存里无界堆积，最终 OOM。

## 5. 背压是什么（Reactive Streams request-n）

WebFlux 基于 Reactive Streams：下游会通过 `request(n)` 表示“我现在最多还能处理 n 个元素/字节块”，上游应该按需生产。

目标：客户端慢时，不要让服务器无限产出并把结果堆在内存里。

## 6. 为什么 outputStreamPublisher 能避免 OOM

本项目的 CSV/ZIP 输出不走 `Flux<Map<...>>`，而是把阻塞式导出逻辑桥接为响应式输出：

- 使用 `DataBufferUtils.outputStreamPublisher(...)` 得到 `Flux<DataBuffer>` 作为响应体
- 在专用线程池中执行阻塞 IO（`ParquetReader`、`ZipOutputStream`、`OutputStream.write`）
- 当客户端慢时，下游 demand 小，底层 `OutputStream.write(...)` 会阻塞，从而让“读 Parquet 的 while 循环”自然变慢
- 客户端断开时，`write(...)` 会抛 `IOException`（broken pipe/connection reset），代码捕获后停止读取，避免无意义继续消耗资源

核心代码见：

- `src/main/java/com/aquarius/wizard/webfluxparquetexportdemo/service/ParquetExportService.java`

补充（小文件/大文件都覆盖的“体验优化”）：
- CSV 写完 header 会立刻 `flush()` 一次，降低“首包延迟”（小输出也能尽快开始下发）
- 后续按“写出字节阈值”低频 `flush()`（配置项 `demo.export.csv-flush-every-bytes`），避免每行 flush 的性能灾难
- ZIP 默认不做周期性 flush（或设置更大的阈值 `demo.export.zip-flush-every-bytes`），避免频繁 flush 影响 deflate 压缩效率

## 7. 为什么 ZipOutputStream 可以边写边压缩

ZIP 的 entry 写入是流式的：

1. `ZipOutputStream.putNextEntry(new ZipEntry("<baseName>.csv"))`
2. 把 CSV 内容直接写到 `ZipOutputStream`（此时会边写边 deflate 压缩）
3. `closeEntry()` / `finish()`

所以不需要先生成完整 CSV 文件，更不需要把 CSV 内容攒在内存里。

另外：`ZipOutputStream.close()` 默认会关闭底层流；在 WebFlux 场景下底层响应流由 Spring 管理，因此实现里用 `StreamUtils.nonClosing(...)` 防止业务代码提前关闭响应流。

## 8. BINARY 输出策略与注意事项

CSV 本质是文本格式；为了保证跨系统/工具可读，本项目约定：

- CSV 编码使用 `UTF-8`
- BINARY 字段输出为 Base64 文本（避免任意 bytes 破坏 UTF-8 编码/CSV 可读性）
- 仍按 RFC4180 最小规则进行 CSV 转义（包含逗号/引号/换行就加引号，引号双写）

## 9. 笔记

- `docs/streaming-export-notes.md`
