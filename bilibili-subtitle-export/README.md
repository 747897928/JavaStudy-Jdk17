# bilibili-subtitle-export

一个独立的 Java CLI 模块，用于把 B 站视频字幕导出为纯文本：

- 输入：视频 URL（支持数组/文件/配置文件）
- 自动链路：`BVID -> aid/cid -> subtitle_url -> body[].content`
- 输出：`{标题}.txt`（一条字幕一行，无时间戳）
- 支持 `java -jar` 直接运行

## 一键打包（jar + 脚本 + config）

在仓库根目录执行：

```bash
./bilibili-subtitle-export/build-bundle.sh
```

会产出目录：`bilibili-subtitle-export/bundle/`，其中包含：

- `bilibili-subtitle-export.jar`
- `run.sh`
- `config.json`

## 运行示例

进入 bundle 目录后直接运行：

```bash
cd bilibili-subtitle-export/bundle
./run.sh
```

默认行为：

- 无参数时，程序会自动尝试读取当前目录的 `config.json`；
- 你也可以通过 `--config` 指定任意配置文件路径。

单个 URL（命令行直接传）：

```bash
./run.sh "https://www.bilibili.com/video/BV1SCzwYfEYm/"
```

URL 数组：

```bash
./run.sh --url-array '["https://www.bilibili.com/video/BV1SCzwYfEYm/","https://www.bilibili.com/video/BV1BVRwY5Ejy/"]' \
  --out-dir ./subtitle-output
```

## 配置文件格式

`config.json`（与 `example-config.json` 同结构）字段：

- `urls`: URL 数组
- `urlFile`: URL 文件路径（JSON 数组或一行一个 URL）
- `outDir`: 输出目录
- `lang`: 字幕语言偏好（默认 `zh`）
- `cookie`: Cookie 字符串（可选）
- `cookieFile`: Cookie 文件路径（可选）
- `timeoutSeconds`: 超时秒数（默认 `20`）

## 指定配置文件（可选）

```bash
./run.sh --config /path/to/your-config.json
```
