package com.aquarius.wizard.bilibilisubtitle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Export Bilibili subtitle body[].content as plain text files.
 */
public final class BilibiliSubtitleExportMain {

    private static final String DEFAULT_CONFIG_FILE = "config.json";
    private static final String VIEW_API = "https://api.bilibili.com/x/web-interface/view?bvid=%s";
    private static final String DM_VIEW_API = "https://api.bilibili.com/x/v2/dm/view?type=1&oid=%d&pid=%d";
    private static final Pattern BVID_PATTERN = Pattern.compile("(BV[0-9A-Za-z]{10})");
    private static final Pattern INVALID_FILENAME_CHARS = Pattern.compile("[\\\\/:*?\"<>|]+");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(20))
            .build();

    public static void main(String[] args) {
        int code = new BilibiliSubtitleExportMain().run(args);
        System.exit(code);
    }

    private int run(String[] args) {
        try {
            CliArgs cliArgs = parseCliArgs(args);
            if (cliArgs.help) {
                printUsage();
                return 0;
            }

            EffectiveConfig config = mergeConfig(cliArgs);
            if (config.urls.isEmpty()) {
                throw new SubtitleExportException("No input URL found.");
            }

            Files.createDirectories(config.outDir);

            int failures = 0;
            for (int i = 0; i < config.urls.size(); i++) {
                String url = config.urls.get(i);
                System.out.printf("[%d/%d] %s%n", i + 1, config.urls.size(), url);
                try {
                    ExportResult result = exportOne(url, config);
                    System.out.printf("  OK  -> %s (%d lines)%n", result.outputPath, result.lineCount);
                } catch (Exception ex) {
                    failures++;
                    System.err.printf("  ERR -> %s%n", ex.getMessage());
                }
            }

            if (failures > 0) {
                System.err.printf("Finished with %d failure(s).%n", failures);
                return 1;
            }
            System.out.println("Finished successfully.");
            return 0;
        } catch (SubtitleExportException ex) {
            System.err.println("Error: " + ex.getMessage());
            return 1;
        } catch (Exception ex) {
            System.err.println("Unexpected error: " + ex.getMessage());
            return 1;
        }
    }

    private ExportResult exportOne(String sourceUrl, EffectiveConfig config) {
        VideoContext context = fetchVideoContext(sourceUrl, config.cookie, config.timeout);
        String subtitleUrl = fetchSubtitleUrl(context, config.lang, config.cookie, config.timeout);
        List<String> lines = fetchSubtitleLines(subtitleUrl, context.normalizedUrl, config.cookie, config.timeout);

        String baseName = buildOutputBaseName(context);
        Path outputPath = nextAvailablePath(config.outDir.resolve(baseName + ".txt"));
        String content = String.join(System.lineSeparator(), lines) + System.lineSeparator();
        try {
            Files.writeString(outputPath, content, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new SubtitleExportException("Failed to write " + outputPath + ": " + ex.getMessage(), ex);
        }

        return new ExportResult(outputPath, lines.size());
    }

    private VideoContext fetchVideoContext(String sourceUrl, String cookie, Duration timeout) {
        BvidResolution resolution = resolveBvid(sourceUrl, cookie, timeout);
        String viewUrl = String.format(VIEW_API, encode(resolution.bvid));
        JsonNode viewPayload = getJson(viewUrl, cookie, resolution.normalizedUrl, timeout);
        assertApiCodeOk(viewPayload, "x/web-interface/view");

        JsonNode data = viewPayload.path("data");
        if (!data.isObject()) {
            throw new SubtitleExportException("Unexpected x/web-interface/view response for " + resolution.bvid);
        }

        long aid = data.path("aid").asLong(0L);
        if (aid <= 0L) {
            throw new SubtitleExportException("Missing aid for " + resolution.bvid);
        }

        String title = safeText(data.path("title"), resolution.bvid);
        JsonNode pages = data.path("pages");
        int pageCount = pages.isArray() && pages.size() > 0 ? pages.size() : 1;
        int pageIndex = clamp(parsePageIndex(resolution.normalizedUrl), 1, pageCount);

        long cid = 0L;
        String partTitle = null;
        if (pages.isArray() && pages.size() > 0) {
            JsonNode pageNode = pages.get(pageIndex - 1);
            cid = pageNode.path("cid").asLong(0L);
            String part = safeText(pageNode.path("part"), "");
            if (!part.isBlank()) {
                partTitle = part;
            }
        }
        if (cid <= 0L) {
            cid = data.path("cid").asLong(0L);
        }
        if (cid <= 0L) {
            throw new SubtitleExportException("Missing cid for " + resolution.bvid);
        }

        return new VideoContext(
                sourceUrl,
                resolution.normalizedUrl,
                resolution.bvid,
                aid,
                cid,
                title,
                pageIndex,
                pageCount,
                partTitle
        );
    }

    private String fetchSubtitleUrl(VideoContext context, String preferredLang, String cookie, Duration timeout) {
        String dmViewUrl = String.format(DM_VIEW_API, context.cid, context.aid);
        JsonNode dmPayload = getJson(dmViewUrl, cookie, context.normalizedUrl, timeout);
        assertApiCodeOk(dmPayload, "x/v2/dm/view");

        JsonNode subtitles = dmPayload.path("data").path("subtitle").path("subtitles");
        if (!subtitles.isArray() || subtitles.size() == 0) {
            throw new SubtitleExportException(
                    String.format(
                            "No subtitle entries for %s (cid=%d). Try cookie if login is required.",
                            context.bvid,
                            context.cid
                    )
            );
        }

        JsonNode selected = pickSubtitleEntry(subtitles, preferredLang);
        if (selected == null) {
            throw new SubtitleExportException("No usable subtitle entry for " + context.bvid);
        }
        String subtitleUrl = safeText(selected.path("subtitle_url"), "");
        if (subtitleUrl.isBlank()) {
            throw new SubtitleExportException("Subtitle entry has empty subtitle_url for " + context.bvid);
        }
        return normalizeSubtitleUrl(subtitleUrl);
    }

    private List<String> fetchSubtitleLines(String subtitleUrl, String referer, String cookie, Duration timeout) {
        JsonNode subtitleJson = getJson(subtitleUrl, cookie, referer, timeout);
        JsonNode body = subtitleJson.path("body");
        if (!body.isArray()) {
            throw new SubtitleExportException("Subtitle JSON missing body[]: " + subtitleUrl);
        }

        List<String> lines = new ArrayList<>();
        for (JsonNode item : body) {
            if (!item.isObject()) {
                continue;
            }
            String content = safeText(item.path("content"), "").trim();
            if (!content.isBlank()) {
                lines.add(content);
            }
        }
        if (lines.isEmpty()) {
            throw new SubtitleExportException("Subtitle body[] has no content lines: " + subtitleUrl);
        }
        return lines;
    }

    private JsonNode pickSubtitleEntry(JsonNode subtitles, String preferredLang) {
        String preferred = preferredLang == null ? "" : preferredLang.trim().toLowerCase(Locale.ROOT);
        if (!preferred.isBlank()) {
            for (JsonNode item : subtitles) {
                if (!item.isObject()) {
                    continue;
                }
                String lan = safeText(item.path("lan"), "").toLowerCase(Locale.ROOT);
                String lanDoc = safeText(item.path("lan_doc"), "").toLowerCase(Locale.ROOT);
                if (lan.contains(preferred) || lanDoc.contains(preferred)) {
                    return item;
                }
            }
        }

        for (JsonNode item : subtitles) {
            if (!item.isObject()) {
                continue;
            }
            String lan = safeText(item.path("lan"), "").toLowerCase(Locale.ROOT);
            String lanDoc = safeText(item.path("lan_doc"), "");
            if (lan.contains("zh") || lanDoc.contains("中文")) {
                return item;
            }
        }

        for (JsonNode item : subtitles) {
            if (item.isObject()) {
                return item;
            }
        }
        return null;
    }

    private BvidResolution resolveBvid(String sourceUrl, String cookie, Duration timeout) {
        Matcher directMatcher = BVID_PATTERN.matcher(sourceUrl);
        if (directMatcher.find()) {
            return new BvidResolution(directMatcher.group(1), sourceUrl);
        }

        String resolvedUrl = resolveFinalUrl(sourceUrl, cookie, timeout);
        Matcher resolvedMatcher = BVID_PATTERN.matcher(resolvedUrl);
        if (resolvedMatcher.find()) {
            return new BvidResolution(resolvedMatcher.group(1), resolvedUrl);
        }
        throw new SubtitleExportException("Cannot find BVID in URL: " + sourceUrl);
    }

    private String resolveFinalUrl(String url, String cookie, Duration timeout) {
        HttpRequest.Builder builder = baseRequestBuilder(url, cookie, "");
        builder.timeout(timeout);
        HttpRequest request = builder.GET().build();
        try {
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            return response.uri().toString();
        } catch (Exception ex) {
            return url;
        }
    }

    private JsonNode getJson(String url, String cookie, String referer, Duration timeout) {
        HttpRequest.Builder builder = baseRequestBuilder(url, cookie, referer);
        builder.timeout(timeout);
        HttpRequest request = builder.GET().build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SubtitleExportException("Request interrupted: " + url, ex);
        } catch (IOException ex) {
            throw new SubtitleExportException("Request failed for " + url + ": " + ex.getMessage(), ex);
        }

        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            String body = response.body() == null ? "" : response.body();
            String snippet = body.length() > 240 ? body.substring(0, 240) : body;
            throw new SubtitleExportException(
                    String.format("HTTP %d for %s. Response snippet: %s", statusCode, url, snippet)
            );
        }

        String responseBody = response.body();
        try {
            return objectMapper.readTree(responseBody);
        } catch (IOException ex) {
            String snippet = responseBody == null ? "" : responseBody;
            if (snippet.length() > 240) {
                snippet = snippet.substring(0, 240);
            }
            throw new SubtitleExportException("Non-JSON response from " + url + ": " + snippet, ex);
        }
    }

    private HttpRequest.Builder baseRequestBuilder(String url, String cookie, String referer) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .header("User-Agent", USER_AGENT)
                .header("Accept", "application/json, text/plain, */*");
        if (cookie != null && !cookie.isBlank()) {
            builder.header("Cookie", cookie);
        }
        if (referer != null && !referer.isBlank()) {
            builder.header("Referer", referer);
        }
        return builder;
    }

    private void assertApiCodeOk(JsonNode payload, String apiName) {
        int code = payload.path("code").asInt(Integer.MIN_VALUE);
        if (code == 0) {
            return;
        }
        String message = safeText(payload.path("message"), "");
        throw new SubtitleExportException(
                String.format("%s failed: code=%d, message=%s", apiName, code, message)
        );
    }

    private CliArgs parseCliArgs(String[] args) {
        CliArgs cliArgs = new CliArgs();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "-h":
                case "--help":
                    cliArgs.help = true;
                    break;
                case "--config":
                    cliArgs.configFile = requireOptionValue(args, ++i, "--config");
                    break;
                case "--url-array":
                    cliArgs.urlArrayText = requireOptionValue(args, ++i, "--url-array");
                    break;
                case "--url-file":
                    cliArgs.urlFile = requireOptionValue(args, ++i, "--url-file");
                    break;
                case "--out-dir":
                    cliArgs.outDir = requireOptionValue(args, ++i, "--out-dir");
                    break;
                case "--lang":
                    cliArgs.lang = requireOptionValue(args, ++i, "--lang");
                    break;
                case "--cookie":
                    cliArgs.cookie = requireOptionValue(args, ++i, "--cookie");
                    break;
                case "--cookie-file":
                    cliArgs.cookieFile = requireOptionValue(args, ++i, "--cookie-file");
                    break;
                case "--timeout":
                    cliArgs.timeoutSecondsText = requireOptionValue(args, ++i, "--timeout");
                    break;
                default:
                    if (arg.startsWith("--")) {
                        throw new SubtitleExportException("Unknown option: " + arg);
                    }
                    cliArgs.positionalUrls.add(arg);
                    break;
            }
        }
        return cliArgs;
    }

    private EffectiveConfig mergeConfig(CliArgs cliArgs) {
        JsonNode configNode = loadConfigNode(cliArgs.configFile);

        String outDirText = firstNonBlank(
                cliArgs.outDir,
                textFrom(configNode, "outDir"),
                "."
        );
        String lang = firstNonBlank(
                cliArgs.lang,
                textFrom(configNode, "lang"),
                "zh"
        );

        String timeoutText = firstNonBlank(
                cliArgs.timeoutSecondsText,
                textFrom(configNode, "timeoutSeconds"),
                "20"
        );
        long timeoutSeconds;
        try {
            timeoutSeconds = Long.parseLong(timeoutText);
        } catch (NumberFormatException ex) {
            throw new SubtitleExportException("Invalid timeout value: " + timeoutText, ex);
        }
        if (timeoutSeconds <= 0) {
            timeoutSeconds = 20;
        }

        String cookie = firstNonBlank(
                cliArgs.cookie,
                textFrom(configNode, "cookie"),
                ""
        );
        String cookieFile = firstNonBlank(
                cliArgs.cookieFile,
                textFrom(configNode, "cookieFile"),
                ""
        );
        if (!cookieFile.isBlank()) {
            Path cookiePath = Path.of(cookieFile);
            if (!Files.exists(cookiePath)) {
                throw new SubtitleExportException("--cookie-file does not exist: " + cookiePath);
            }
            try {
                String fileCookie = Files.readString(cookiePath, StandardCharsets.UTF_8).trim();
                if (!fileCookie.isBlank()) {
                    cookie = fileCookie;
                }
            } catch (IOException ex) {
                throw new SubtitleExportException("Failed to read cookie file: " + cookiePath, ex);
            }
        }

        LinkedHashSet<String> urls = new LinkedHashSet<>();
        boolean hasCliUrlInputs = hasCliUrlInputs(cliArgs);
        if (!hasCliUrlInputs) {
            collectUrlsFromConfigNode(urls, configNode);
        }
        collectUrlsFromCli(urls, cliArgs);

        return new EffectiveConfig(
                new ArrayList<>(urls),
                Path.of(outDirText).toAbsolutePath().normalize(),
                lang,
                cookie,
                Duration.ofSeconds(timeoutSeconds)
        );
    }

    private boolean hasCliUrlInputs(CliArgs cliArgs) {
        return !cliArgs.positionalUrls.isEmpty()
                || (cliArgs.urlArrayText != null && !cliArgs.urlArrayText.isBlank())
                || (cliArgs.urlFile != null && !cliArgs.urlFile.isBlank());
    }

    private JsonNode loadConfigNode(String configFile) {
        if (configFile == null || configFile.isBlank()) {
            Path defaultConfigPath = Path.of(DEFAULT_CONFIG_FILE);
            if (!Files.exists(defaultConfigPath)) {
                return objectMapper.createObjectNode();
            }
            return readConfigFile(defaultConfigPath, "default config");
        }
        Path configPath = Path.of(configFile);
        if (!Files.exists(configPath)) {
            throw new SubtitleExportException("--config does not exist: " + configPath);
        }
        return readConfigFile(configPath, "--config");
    }

    private JsonNode readConfigFile(Path configPath, String sourceName) {
        try {
            String text = Files.readString(configPath, StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(text);
            if (node == null || !node.isObject()) {
                throw new SubtitleExportException(sourceName + " must be a JSON object: " + configPath);
            }
            return node;
        } catch (IOException ex) {
            throw new SubtitleExportException("Failed to read config file: " + configPath, ex);
        }
    }

    private void collectUrlsFromConfigNode(LinkedHashSet<String> urls, JsonNode configNode) {
        if (!configNode.isObject()) {
            return;
        }

        JsonNode urlsNode = configNode.path("urls");
        if (!urlsNode.isMissingNode()) {
            urls.addAll(parseUrlArrayNode(urlsNode, "config.urls"));
        }

        String urlFile = textFrom(configNode, "urlFile");
        if (!urlFile.isBlank()) {
            urls.addAll(loadUrlsFromFile(Path.of(urlFile)));
        }
    }

    private void collectUrlsFromCli(LinkedHashSet<String> urls, CliArgs cliArgs) {
        if (cliArgs.positionalUrls.size() == 1 && cliArgs.positionalUrls.get(0).trim().startsWith("[")) {
            urls.addAll(parseUrlArrayText(cliArgs.positionalUrls.get(0), "positional argument"));
        } else {
            for (String url : cliArgs.positionalUrls) {
                String trimmed = url == null ? "" : url.trim();
                if (!trimmed.isBlank()) {
                    urls.add(trimmed);
                }
            }
        }

        if (cliArgs.urlArrayText != null && !cliArgs.urlArrayText.isBlank()) {
            urls.addAll(parseUrlArrayText(cliArgs.urlArrayText, "--url-array"));
        }

        if (cliArgs.urlFile != null && !cliArgs.urlFile.isBlank()) {
            urls.addAll(loadUrlsFromFile(Path.of(cliArgs.urlFile)));
        }
    }

    private List<String> loadUrlsFromFile(Path filePath) {
        if (!Files.exists(filePath)) {
            throw new SubtitleExportException("--url-file does not exist: " + filePath);
        }
        final String text;
        try {
            text = Files.readString(filePath, StandardCharsets.UTF_8).trim();
        } catch (IOException ex) {
            throw new SubtitleExportException("Failed to read --url-file: " + filePath, ex);
        }

        if (text.isBlank()) {
            return List.of();
        }
        if (text.startsWith("[")) {
            return parseUrlArrayText(text, filePath.toString());
        }

        List<String> urls = new ArrayList<>();
        for (String line : text.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isBlank() || trimmed.startsWith("#")) {
                continue;
            }
            urls.add(trimmed);
        }
        return urls;
    }

    private List<String> parseUrlArrayText(String text, String source) {
        try {
            JsonNode node = objectMapper.readTree(text);
            return parseUrlArrayNode(node, source);
        } catch (IOException ex) {
            throw new SubtitleExportException(source + " is not a valid JSON array: " + ex.getMessage(), ex);
        }
    }

    private List<String> parseUrlArrayNode(JsonNode node, String source) {
        if (!node.isArray()) {
            throw new SubtitleExportException(source + " must be a JSON array.");
        }
        List<String> urls = new ArrayList<>();
        for (JsonNode item : node) {
            if (!item.isTextual()) {
                continue;
            }
            String trimmed = item.asText("").trim();
            if (!trimmed.isBlank()) {
                urls.add(trimmed);
            }
        }
        return urls;
    }

    private String buildOutputBaseName(VideoContext context) {
        if (context.pageCount > 1) {
            String base = context.title + "-P" + context.pageIndex;
            if (context.partTitle != null && !context.partTitle.isBlank() && !context.partTitle.equals(context.title)) {
                base = base + "-" + context.partTitle;
            }
            return sanitizeFileName(base);
        }
        return sanitizeFileName(context.title);
    }

    private String sanitizeFileName(String raw) {
        String cleaned = INVALID_FILENAME_CHARS.matcher(raw).replaceAll("_");
        cleaned = WHITESPACE.matcher(cleaned).replaceAll(" ").trim();
        while (cleaned.endsWith(".")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1);
        }
        if (cleaned.isBlank()) {
            cleaned = "untitled";
        }
        if (cleaned.length() > 150) {
            cleaned = cleaned.substring(0, 150);
        }
        return cleaned;
    }

    private Path nextAvailablePath(Path path) {
        if (!Files.exists(path)) {
            return path;
        }
        String fileName = path.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');
        String stem = dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
        String suffix = dotIndex > 0 ? fileName.substring(dotIndex) : "";

        int index = 2;
        while (true) {
            Path candidate = path.getParent().resolve(stem + "_" + index + suffix);
            if (!Files.exists(candidate)) {
                return candidate;
            }
            index++;
        }
    }

    private int parsePageIndex(String url) {
        try {
            URI uri = URI.create(url);
            Map<String, String> queryMap = parseQuery(uri.getRawQuery());
            String pageText = queryMap.getOrDefault("p", "");
            if (pageText.isBlank()) {
                return 1;
            }
            int page = Integer.parseInt(pageText);
            return page > 0 ? page : 1;
        } catch (Exception ex) {
            return 1;
        }
    }

    private Map<String, String> parseQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Map.of();
        }
        java.util.LinkedHashMap<String, String> result = new java.util.LinkedHashMap<>();
        String[] parts = rawQuery.split("&");
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            String[] kv = part.split("=", 2);
            String key = decode(kv[0]);
            String value = kv.length > 1 ? decode(kv[1]) : "";
            result.putIfAbsent(key, value);
        }
        return result;
    }

    private String normalizeSubtitleUrl(String subtitleUrl) {
        if (subtitleUrl.startsWith("//")) {
            return "https:" + subtitleUrl;
        }
        if (subtitleUrl.startsWith("http://")) {
            return "https://" + subtitleUrl.substring("http://".length());
        }
        return subtitleUrl;
    }

    private String requireOptionValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new SubtitleExportException("Missing value for " + option);
        }
        return args[index];
    }

    private String textFrom(JsonNode node, String fieldName) {
        if (node == null || !node.isObject()) {
            return "";
        }
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) {
            return "";
        }
        if (field.isNumber()) {
            return field.asText();
        }
        return field.asText("").trim();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }

    private String safeText(JsonNode node, String fallback) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return fallback;
        }
        String text = node.asText();
        return text == null ? fallback : text;
    }

    private int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar bilibili-subtitle-export-*-jar-with-dependencies.jar [options] [url1 url2 ...]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --config <file>       JSON config file (default: ./config.json if exists)");
        System.out.println("  --url-array <json>    URL JSON array string");
        System.out.println("  --url-file <file>     URL file (JSON array or one URL per line)");
        System.out.println("  --out-dir <dir>       Output directory (default: .)");
        System.out.println("  --lang <lang>         Preferred subtitle language (default: zh)");
        System.out.println("  --cookie <cookie>     Optional Cookie header");
        System.out.println("  --cookie-file <file>  Optional cookie file");
        System.out.println("  --timeout <seconds>   HTTP timeout in seconds (default: 20)");
        System.out.println("  -h, --help            Show this help");
    }

    private static final class CliArgs {
        private boolean help;
        private String configFile = "";
        private String urlArrayText = "";
        private String urlFile = "";
        private String outDir = "";
        private String lang = "";
        private String cookie = "";
        private String cookieFile = "";
        private String timeoutSecondsText = "";
        private final List<String> positionalUrls = new ArrayList<>();
    }

    private static final class EffectiveConfig {
        private final List<String> urls;
        private final Path outDir;
        private final String lang;
        private final String cookie;
        private final Duration timeout;

        private EffectiveConfig(List<String> urls, Path outDir, String lang, String cookie, Duration timeout) {
            this.urls = Objects.requireNonNull(urls);
            this.outDir = Objects.requireNonNull(outDir);
            this.lang = Objects.requireNonNull(lang);
            this.cookie = Objects.requireNonNull(cookie);
            this.timeout = Objects.requireNonNull(timeout);
        }
    }

    private static final class BvidResolution {
        private final String bvid;
        private final String normalizedUrl;

        private BvidResolution(String bvid, String normalizedUrl) {
            this.bvid = bvid;
            this.normalizedUrl = normalizedUrl;
        }
    }

    private static final class VideoContext {
        private final String sourceUrl;
        private final String normalizedUrl;
        private final String bvid;
        private final long aid;
        private final long cid;
        private final String title;
        private final int pageIndex;
        private final int pageCount;
        private final String partTitle;

        private VideoContext(
                String sourceUrl,
                String normalizedUrl,
                String bvid,
                long aid,
                long cid,
                String title,
                int pageIndex,
                int pageCount,
                String partTitle
        ) {
            this.sourceUrl = sourceUrl;
            this.normalizedUrl = normalizedUrl;
            this.bvid = bvid;
            this.aid = aid;
            this.cid = cid;
            this.title = title;
            this.pageIndex = pageIndex;
            this.pageCount = pageCount;
            this.partTitle = partTitle;
        }
    }

    private static final class ExportResult {
        private final Path outputPath;
        private final int lineCount;

        private ExportResult(Path outputPath, int lineCount) {
            this.outputPath = outputPath;
            this.lineCount = lineCount;
        }
    }

    private static final class SubtitleExportException extends RuntimeException {
        private SubtitleExportException(String message) {
            super(message);
        }

        private SubtitleExportException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
