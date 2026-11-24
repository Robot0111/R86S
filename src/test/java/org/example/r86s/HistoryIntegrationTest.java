package org.example.r86s;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HistoryIntegrationTest {
    private static final String FIRST = "r86s-20260101T000001Z-aaaaaaaaaaaa";
    private static final String SECOND = "r86s-20260101T000002Z-bbbbbbbbbbbb";
    private static final String THIRD = "r86s-20260101T000003Z-cccccccccccc";
    private static final String EVENT_ONE = "a".repeat(64);
    private static final String EVENT_TWO = "b".repeat(64);
    private static final String ORDINARY = "c".repeat(64);
    private static final String SAMPLE = "a".repeat(32);
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @TempDir
    Path root;

    private AppConfig.Database database;
    private HistoryStore store;
    private Instant time;

    @BeforeEach
    void prepare() throws Exception {
        Files.createDirectories(root.resolve("state"));
        Files.createDirectories(root.resolve("batches/sent"));
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=Oracle;DB_CLOSE_DELAY=-1";
        database = new AppConfig.Database(url, "sa", "");
        store = new HistoryStore(database, "r86s", JSON, root.resolve("state"), "");
        time = Instant.now().minus(Duration.ofDays(2)).truncatedTo(java.time.temporal.ChronoUnit.MINUTES).plusSeconds(15);
        try (var connection = database.connect(); var statement = connection.createStatement()) {
            statement.execute("CREATE ALIAS JSON_VALUE FOR 'org.example.r86s.HistoryIntegrationTest.jsonNumber'");
            for (String filename : List.of("sql/oracle-schema.sql", "sql/oracle-traffic-rules.sql")) {
                for (String command : Files.readString(Path.of(filename)).split(";")) {
                    if (!command.isBlank()) statement.execute(command);
                }
            }
        }
    }

    public static BigDecimal jsonNumber(String document, String path) throws IOException {
        JsonNode row = JSON.readTree(document);
        JsonNode value = switch (path) {
            case "$.load[0]" -> row.path("load").path(0);
            case "$.memory.SwapTotal" -> row.path("memory").path("SwapTotal");
            case "$.memory.SwapFree" -> row.path("memory").path("SwapFree");
            default -> throw new IllegalArgumentException("Unexpected JSON path: " + path);
        };
        return value.isNumber() ? value.decimalValue() : null;
    }

    @Test
    void batchesAreVerifiedAndDeduplicatedBeforeLocalCleanup() throws Exception {
        List<Map<String, Object>> important = List.of(
                event(EVENT_ONE, "kernel", "out of memory"),
                event(EVENT_TWO, "network", "eth2: link is down"),
                event(ORDINARY, "proxy-tunnel", "ordinary access"));
        Path first = batch(FIRST, important, List.of(sample()), true);
        Path second = batch(SECOND, important, List.of(sample()), true);
        Path unconfirmed = batch(THIRD, important, List.of(sample()), false);
        Path closed = closedRaw("network", event(EVENT_ONE, "kernel", "out of memory"));
        Path orphan = closedRaw("system", event("d".repeat(64), "system", "capture only"));
        Path active = root.resolve("raw").resolve(Instant.now().atOffset(ZoneOffset.UTC).toLocalDate().toString())
                .resolve("kernel.jsonl");
        Files.createDirectories(active.getParent());
        Files.copy(closed, active);
        Files.setLastModifiedTime(active, FileTime.from(time));
        Path partial = closedRaw("partial", event(EVENT_ONE, "kernel", "out of memory"));
        Files.writeString(partial, "{\"partial\":", java.nio.file.StandardOpenOption.APPEND);
        Files.setLastModifiedTime(partial, FileTime.from(time));

        importer(true).importOnce();

        assertFalse(Files.exists(first));
        assertFalse(Files.exists(second));
        assertTrue(Files.exists(unconfirmed));
        assertFalse(Files.exists(closed));
        assertTrue(Files.exists(active));
        assertTrue(Files.exists(orphan));
        assertTrue(Files.exists(partial));
        assertEquals(2, rows("r86s_ingest_batch"));
        assertEquals(3, rows("r86s_event"));
        assertEquals(1, rows("r86s_metric_sample"));

        long from = HistoryData.windowStart(time.toEpochMilli());
        long to = from + 2 * HistoryData.WINDOW_MS;
        var windows = store.windows(from, to);
        assertEquals(2, windows.size());
        assertEquals(1, windows.getFirst().sampleCount());
        assertEquals(32.5, windows.getFirst().cpuMaxPercent());
        assertEquals(1, windows.getFirst().eth2DownSamples());
        assertNull(windows.getFirst().load1Max());
        assertNull(windows.getFirst().swapUsedMaxBytes());
        assertEquals(0, windows.getLast().sampleCount());
        assertNull(windows.getLast().cpuMaxPercent());

        var page = store.events(from, to, null, null, 1, false);
        assertEquals(EVENT_TWO, page.items().getFirst().id());
        assertNotNull(page.next());
        var next = store.events(from, to, null, page.next(), 1, false);
        assertEquals(EVENT_ONE, next.items().getFirst().id());
        assertNull(next.next());
        assertEquals(3, store.events(from, to, null, null, 20, true).items().size());
        assertEquals("out of memory", store.event(EVENT_ONE).raw());
        assertEquals("r86s", store.event(EVENT_ONE).record().path("host_id").asText());
    }

    @Test
    void oneArchivePreservesGroupedAndLargeJsonRows() throws Exception {
        var events = new java.util.ArrayList<Map<String, Object>>();
        for (int i = 0; i < 260; i++) {
            events.add(event("%064x".formatted(i), "network", "eth2 link down " + i));
        }
        String largeRaw = "Disk alert \u78c1\u76d8 \ud83d\udcbe ".repeat(400);
        events.add(event(EVENT_ONE, "kernel", largeRaw));
        events.add(event(EVENT_ONE, "kernel", "same ID already seen"));
        events.add(event(EVENT_TWO, "network", "pppoe-wan link is up"));
        Path archive = batch(FIRST, events, List.of(sample()), true);

        importer(true).importOnce();

        assertFalse(Files.exists(archive));
        assertEquals(262, rows("r86s_event"));
        assertEquals(1, rows("r86s_metric_sample"));
        assertEquals(1, rows("r86s_ingest_batch"));
        assertEquals(largeRaw, store.event(EVENT_ONE).raw());
        assertEquals("pppoe-wan link is up", store.event(EVENT_TWO).raw());
    }

    @Test
    void failedBatchRollsBackDataAndLeavesFilesForRetry() throws Exception {
        Path archive = batch(FIRST, List.of(event(EVENT_ONE, "kernel", "out of memory")), List.of(sample()), true);
        Path corrupt = batch(SECOND, List.of(event(EVENT_TWO, "network", "eth2: link is down")), List.of(), true);
        Files.write(corrupt, new byte[]{1, 2, 3}, java.nio.file.StandardOpenOption.APPEND);
        execute("ALTER TABLE r86s_ingest_batch ADD CONSTRAINT failed_batch CHECK (event_count < 0)");

        assertThrows(SQLException.class, () -> importer(true).importOnce());
        assertTrue(Files.exists(archive));
        assertEquals(0, rows("r86s_event"));
        assertEquals(0, rows("r86s_metric_sample"));
        assertEquals(0, rows("r86s_ingest_batch"));

        execute("ALTER TABLE r86s_ingest_batch DROP CONSTRAINT failed_batch");
        importer(false).importOnce();
        assertTrue(Files.exists(archive));
        assertTrue(Files.exists(corrupt));
        assertEquals(1, rows("r86s_ingest_batch"));
        importer(true).importOnce();
        assertFalse(Files.exists(archive));
        assertTrue(Files.exists(corrupt));
        assertEquals(1, rows("r86s_event"));
    }

    @Test
    void oracleRetentionExpiresOnlyMatchingHostData() throws Exception {
        batch(FIRST, List.of(event(EVENT_ONE, "kernel", "out of memory")), List.of(sample()), true);
        importer(false).importOnce();
        var other = new HistoryStore(database, "another-host", JSON);
        try (var connection = store.connect()) {
            connection.setAutoCommit(false);
            other.writeBatch(connection, new BatchArchive.Batch(FIRST, "a".repeat(64), time.toEpochMilli(),
                    List.of(HistoryData.metric(JSON.valueToTree(sample()))),
                    List.of(HistoryData.event(JSON.valueToTree(event(EVENT_ONE, "kernel", "out of memory"))))));
            store.expireHistory(connection, time.plusSeconds(2).toEpochMilli());
        }
        assertNull(store.event(EVENT_ONE));
        assertNotNull(other.event(EVENT_ONE));
        assertEquals(1, rows("r86s_ingest_batch"));
        assertEquals(1, rows("r86s_metric_sample"));
    }

    @Test
    void trafficUsesDeltasAcrossObservedPairsAndKeepsGaps() throws Exception {
        Instant start = Instant.ofEpochMilli(HistoryData.windowStart(time.toEpochMilli()));
        batch(FIRST, List.of(), List.of(
                trafficSample(start.plusSeconds(15), 1, "boot-a", "counter-a", 100, 40),
                trafficSample(start.plusSeconds(45), 2, "boot-a", "counter-a", 130, 62),
                trafficSample(start.plusSeconds(75), 3, "boot-a", "counter-b", 150, 100),
                trafficSample(start.plusSeconds(105), 4, "boot-a", "counter-b", 170, 107),
                trafficSample(start.plusSeconds(135), 5, "boot-b", "counter-b", 180, 110),
                trafficSample(start.plusSeconds(165), 6, "boot-b", "counter-b", 5, 2),
                trafficSample(start.plusSeconds(195), 7, "boot-b", "counter-b", 15, 4),
                trafficSample(start.plusSeconds(615), 8, "boot-b", "counter-b", 44, 9),
                trafficSample(start.plusSeconds(645), 9, "boot-b", "counter-b", 46, 12)), true);
        importer(false).importOnce();

        var windows = store.traffic(start.toEpochMilli(), start.plusSeconds(900).toEpochMilli(), "pppoe-wan");
        assertEquals(3, windows.size());
        assertEquals(60, windows.getFirst().rxBytes());
        assertEquals(31, windows.getFirst().txBytes());
        assertEquals(3, windows.getFirst().sampleCount());
        assertNull(windows.get(1).rxBytes());
        assertNull(windows.get(1).txBytes());
        assertEquals(2, windows.getLast().rxBytes());
        assertEquals(3, windows.getLast().txBytes());
        assertEquals(1, windows.getLast().sampleCount());
        assertNull(store.traffic(start.toEpochMilli(), start.plusSeconds(300).toEpochMilli(), "forwarded:wan")
                .getFirst().rxBytes());
    }

    @Test
    void clientTrafficKeepsRegionTotalsDeviceChoicesAndMissingWindowsDistinct() throws Exception {
        Instant start = Instant.ofEpochMilli(HistoryData.windowStart(time.toEpochMilli()));
        Map<String, Object> clientSample = new HashMap<>(sample());
        clientSample.put("sampled_at", start.plusSeconds(15).toString());
        clientSample.put("client_traffic", Map.of("state", "ok", "partial", false, "flows", List.of(
                clientFlow("internet", "cn", "direct", "192.168.1.2", "10:11:12:13:14:15", "203.0.113.1", 10, 2),
                clientFlow("internet", "foreign", "proxy", "192.168.1.2", "10:11:12:13:14:15", "203.0.113.2", 20, 3),
                clientFlow("internet", "unknown", "proxy", "192.168.1.3", "", "203.0.113.2", 4, 1),
                clientFlow("internet", "unknown", "unknown", "192.168.1.3", "", "203.0.113.3", 5, 0),
                clientFlow("internal", "internal", "unknown", "192.168.1.2", "10:11:12:13:14:15", "192.168.6.1", 35, 6))));
        Map<String, Object> noFlows = new HashMap<>(clientSample);
        noFlows.put("sample_id", "b".repeat(32));
        noFlows.put("sampled_at", start.plusSeconds(315).toString());
        noFlows.put("client_traffic", Map.of("state", "ok", "partial", true, "flows", List.of()));
        Map<String, Object> oldMetric = new HashMap<>(sample());
        oldMetric.put("sample_id", "c".repeat(32));
        oldMetric.put("sampled_at", start.plusSeconds(615).toString());
        oldMetric.put("proxy_traffic", Map.of("rx_bytes", 9, "tx_bytes", 8));
        var exactSample = new HashMap<>(clientSample);
        exactSample.put("sample_id", "e".repeat(32));
        exactSample.put("sampled_at", start.plusSeconds(915).toString());
        exactSample.put("proxy_traffic", Map.of("rx_bytes", 6, "tx_bytes", 5));
        batch(FIRST, List.of(), List.of(clientSample, noFlows, oldMetric, exactSample), true);
        importer(false).importOnce();

        long from = start.toEpochMilli();
        long to = start.plusSeconds(900).toEpochMilli();
        var complete = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of()));
        assertEquals("partial", complete.state());
        assertEquals(39L, complete.windows().getFirst().rxBytes());
        assertEquals(6L, complete.windows().getFirst().txBytes());
        assertEquals(0L, complete.windows().get(1).rxBytes());
        assertNull(complete.windows().get(2).rxBytes());
        assertEquals(2, complete.quality().coveredSamples());
        assertEquals(1, complete.quality().partialSamples());
        assertEquals(1, complete.quality().missingWindows());
        assertEquals(2, complete.devices().size());

        store.trafficRules().insert(TrafficRule.create(null, "ip", "203.0.113.2", "已确认服务"));
        store.trafficRules().insert(TrafficRule.create(null, "mac", "10-11-12-13-14-15", "测试设备"));
        store.trafficRules().insert(TrafficRule.create(null, "client_ip", "192.168.1.2", "不覆盖 MAC"));
        store.trafficRules().insert(TrafficRule.create(null, "client_ip", "192.168.1.3", "VPN 设备"));
        var proxy = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of("category", "aws")));
        assertEquals(24L, proxy.windows().getFirst().rxBytes());
        assertEquals(4L, proxy.windows().getFirst().txBytes());
        assertEquals(1, proxy.services().size());
        assertEquals("已确认服务", proxy.services().getFirst().name());
        assertEquals(2, proxy.deviceServices().size());
        assertEquals(List.of("203.0.113.2"), proxy.deviceServices().getFirst().targets());
        assertEquals(List.of("203.0.113.2"), proxy.deviceServices().getFirst().targetIps());
        assertFalse(proxy.deviceServices().getFirst().targetIpsTruncated());
        assertEquals("测试设备", proxy.deviceServices().getFirst().deviceName());
        assertEquals("测试设备", proxy.devices().getFirst().deviceName());
        assertEquals("VPN 设备", proxy.deviceServices().get(1).deviceName());
        assertEquals("VPN 设备", proxy.devices().get(1).deviceName());
        assertEquals(45L, store.awsUsage(from, to).totalBytes());
        var monthly = store.awsUsage(from, start.plusSeconds(1_200).toEpochMilli());
        assertEquals(56L, monthly.totalBytes());
        assertEquals(2, monthly.counterSamples());
        assertEquals(2, monthly.estimatedSamples());
        assertEquals(3_600, store.clientTraffic(from - Duration.ofDays(30).toMillis(), to,
                ClientTraffic.Filters.from(Map.of())).windowSeconds());

        var filtered = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of(
                "client", "192.168.1.3", "route", "proxy", "region", "unknown")));
        assertEquals(4L, filtered.windows().getFirst().rxBytes());
        assertEquals(4L, filtered.totals().get(2).rxBytes());
        assertEquals(1, filtered.destinations().size());
        assertEquals("203.0.113.2", filtered.destinations().getFirst().ip());

        var regionOnly = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of("region", "unknown")));
        assertEquals(9L, regionOnly.windows().getFirst().rxBytes());
        assertEquals(10L, regionOnly.totals().getFirst().rxBytes());
        assertEquals(20L, regionOnly.totals().get(1).rxBytes());

        var selected = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of("client", "10:11:12:13:14:15")));
        assertEquals(30L, selected.windows().getFirst().rxBytes());
        assertEquals(2, selected.devices().size());
        var internal = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of("scope", "internal")));
        assertEquals(35L, internal.windows().getFirst().rxBytes());
        assertEquals("internal", internal.totals().getFirst().region());
        var unavailable = store.clientTraffic(start.plusSeconds(600).toEpochMilli(), to,
                ClientTraffic.Filters.from(Map.of()));
        assertEquals("unavailable", unavailable.state());
        assertNull(unavailable.totals().getFirst().rxBytes());
    }

    @Test
    void clientHostnamesLabelGroupedAddressesWithoutDefiningDeviceIdentity() throws Exception {
        Instant start = Instant.ofEpochMilli(HistoryData.windowStart(time.toEpochMilli()));
        String mac = "30:8a:f7:47:41:3c";
        String target = "203.0.113.2";
        var knownV4 = new HashMap<>(clientFlow("internet", "cn", "direct", "192.168.2.113", mac, target, 10, 2));
        knownV4.put("client_hostname", "workstation.lan.");
        var knownV6 = new HashMap<>(clientFlow("internet", "cn", "direct", "2001:db8:1::7", mac, target, 20, 3));
        knownV6.put("client_hostname", "<b>invalid</b>");
        var unrelatedV6 = new HashMap<>(clientFlow("internet", "cn", "direct", "2001:db8:1::8", "", target, 4, 1));
        unrelatedV6.put("client_hostname", "workstation.lan");
        var sample = new HashMap<>(sample());
        sample.put("sampled_at", start.plusSeconds(15).toString());
        sample.put("client_traffic", Map.of("state", "ok", "flows", List.of(knownV4, knownV6, unrelatedV6)));
        batch(FIRST, List.of(), List.of(sample), true);
        importer(false).importOnce();

        long from = start.toEpochMilli();
        long to = start.plusSeconds(300).toEpochMilli();
        var result = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of()));
        assertEquals(2, result.devices().size());
        assertEquals(mac, result.devices().getFirst().id());
        assertEquals(30L, result.devices().getFirst().rxBytes());
        assertEquals("workstation.lan", result.devices().getFirst().deviceName());
        assertEquals("workstation.lan", result.deviceServices().getFirst().deviceName());
        assertEquals("2001:db8:1::8", result.devices().getLast().id());

        store.trafficRules().insert(TrafficRule.create(null, "mac", mac, "自定义名称"));
        var named = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of("client", mac)));
        assertEquals("自定义名称", named.devices().getFirst().deviceName());
        assertEquals("自定义名称", named.deviceServices().getFirst().deviceName());
        assertEquals(30L, named.windows().getFirst().rxBytes());
    }

    @Test
    void hostingProviderFallbackAggregatesMatchingPtrWithoutChangingApplicationRules() throws Exception {
        Instant start = Instant.ofEpochMilli(HistoryData.windowStart(time.toEpochMilli()));
        String mac = "30:8a:f7:47:41:3c";
        var first = new HashMap<>(clientFlow("internet", "cn", "direct", "192.168.2.113", mac, "49.4.46.116", 10, 1));
        first.put("target_name", "ECS-49-4-46-116.compute.hwclouds-dns.com.");
        first.put("target_name_source", "ptr");
        var second = new HashMap<>(first);
        second.put("target_ip", "124.70.116.90");
        second.put("target_name", "ecs-124-70-116-90.compute.hwclouds-dns.com");
        var wrongIp = new HashMap<>(first);
        wrongIp.put("target_ip", "157.148.48.223");
        var wrongSuffix = new HashMap<>(first);
        wrongSuffix.put("target_name", "ecs-49-4-46-116.compute.hwclouds-dns.com.example.net");
        var application = new HashMap<>(first);
        application.put("target_name", "www.youtube.com");
        application.put("target_name_source", "xray");
        var sample = new HashMap<>(sample());
        sample.put("sampled_at", start.plusSeconds(15).toString());
        sample.put("client_traffic", Map.of("state", "ok", "flows", List.of(first, second, wrongIp, wrongSuffix, application)));
        batch(FIRST, List.of(), List.of(sample), true);
        importer(false).importOnce();

        long from = start.toEpochMilli();
        long to = start.plusSeconds(300).toEpochMilli();
        var grouped = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of()));
        var counts = new HashMap<String, Long>();
        grouped.services().forEach(service -> counts.put(service.name(), service.rxBytes()));
        assertEquals(Map.of("华为云（托管）", 20L, "未知", 20L, "YouTube", 10L), counts);

        store.trafficRules().insert(TrafficRule.create(null, "ip", "124.70.116.90", "手动识别"));
        var customized = store.clientTraffic(from, to, ClientTraffic.Filters.from(Map.of()));
        assertTrue(customized.services().stream().anyMatch(service -> service.name().equals("手动识别")));

        var classifier = new TrafficServices(null, List.of());
        var cache = new HashMap<String, String>();
        assertEquals("百度智能云（托管）", classifier.identify("203.0.113.1", "example.bj.bcebos.com", "dns", cache));
        assertEquals("百度", classifier.identify("203.0.113.1", "www.baidu.com", "dns", cache));
        assertEquals("未知", classifier.identify("203.0.113.1", "www.baidu.com", "ptr", cache));
        assertEquals("阿里云（托管）", classifier.identify("203.0.113.1", "oss-cn-hangzhou.aliyuncs.com", "dns", cache));
        assertEquals("腾讯云（托管）", classifier.identify("203.0.113.1", "example.cos.ap-guangzhou.myqcloud.com", "xray", cache));
        assertEquals("火山引擎（托管）", classifier.identify("203.0.113.1", "tos-s3-cn-beijing.volces.com", "dns", cache));
        assertEquals("UCloud（托管）", classifier.identify("203.0.113.1", "example.cn-bj.ufileos.com", "xray", cache));
        assertEquals("京东云（托管）", classifier.identify("203.0.113.1", "vm.jdcloud-api.com", "dns", cache));
        assertEquals("AWS（托管）", classifier.identify("203.0.113.1", "ec2-203-0-113-1.compute-1.amazonaws.com", "ptr", cache));
        assertEquals("Google Cloud（托管）", classifier.identify("203.0.113.1", "1.113.0.203.bc.googleusercontent.com", "ptr", cache));
        assertEquals("Azure（托管）", classifier.identify("203.0.113.1", "workload.azurewebsites.net", "dns", cache));
        assertEquals("Oracle Cloud（托管）", classifier.identify("203.0.113.1", "objectstorage.us-ashburn-1.oraclecloud.com", "dns", cache));
        assertEquals("未知", classifier.identify("203.0.113.1", "fakealiyuncs.com", "dns", cache));
    }

    @Test
    void trafficRulesRequireAuthorizationAndEnforceCanonicalKeys() throws Exception {
        String token = "a".repeat(64);
        Files.writeString(root.resolve("state/client-traffic.token"), token + "\n");
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/", new MonitorApi(JSON, new StatusReader(root.resolve("state"), JSON), store)::handle);
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/api/v1/traffic-rules";
            assertEquals(401, get(client, url).statusCode());
            assertEquals(0, JSON.readTree(get(client, url, token).body()).path("items").size());
            String mac = """
                    {"kind":"mac","target":"AA-BB-CC-DD-EE-FF","label":"桌面电脑"}
                    """;
            assertEquals(401, sendRule(client, url, null, "POST", mac).statusCode());
            var added = sendRule(client, url, token, "POST", mac);
            assertEquals(201, added.statusCode());
            var created = JSON.readTree(added.body());
            assertEquals("aa:bb:cc:dd:ee:ff", created.path("target").asText());
            String id = created.path("id").asText();
            assertEquals(409, sendRule(client, url, token, "POST", mac).statusCode());
            assertEquals(400, sendRule(client, url, token, "POST", "{\"kind\":\"mac\",\"target\":\"invalid\",\"label\":\"name\"}").statusCode());
            assertEquals(400, sendRule(client, url, token, "POST", mac.repeat(200)).statusCode());
            assertEquals(200, sendRule(client, url + "/" + id, token, "PUT", mac.replace("桌面电脑", "客厅电脑")).statusCode());
            assertEquals("客厅电脑", JSON.readTree(get(client, url, token).body()).path("items").get(0).path("label").asText());
            assertEquals(0, new TrafficRuleStore(database, "other-router").list().size());
            assertFalse(new TrafficRuleStore(database, "other-router").delete(id));
            assertEquals(204, sendRule(client, url + "/" + id, token, "DELETE", null).statusCode());
            assertEquals(404, sendRule(client, url + "/" + id, token, "DELETE", null).statusCode());
            String serviceIp = "{\"kind\":\"ip\",\"target\":\"10.10.0.2\",\"label\":\"vpn\"}";
            var service = sendRule(client, url, token, "POST", serviceIp);
            assertEquals(201, service.statusCode());
            String vpnId = JSON.readTree(service.body()).path("id").asText();
            String deviceIp = serviceIp.replace("\"ip\"", "\"client_ip\"");
            var changed = sendRule(client, url + "/" + vpnId, token, "PUT", deviceIp);
            assertEquals(200, changed.statusCode());
            assertEquals("client_ip", JSON.readTree(changed.body()).path("kind").asText());
            assertEquals("vpn", new TrafficServices(null, store.trafficRules().list()).deviceName(null, "10.10.0.2"));
            assertEquals(409, sendRule(client, url, token, "POST", deviceIp).statusCode());
            assertEquals(201, sendRule(client, url, token, "POST", serviceIp).statusCode());
            var ipv6 = sendRule(client, url, token, "POST", deviceIp.replace("10.10.0.2", "2001:db8::7"));
            assertEquals(201, ipv6.statusCode());
            assertEquals("vpn", new TrafficServices(null, store.trafficRules().list()).deviceName(null, "2001:db8::7"));
            try (var connection = database.connect(); var statement = connection.createStatement()) {
                statement.execute("DROP TABLE r86s_traffic_rule");
            }
            assertEquals("rules_schema_missing", JSON.readTree(get(client, url, token).body()).path("error").asText());
            assertTrue(store.trafficRules().forTraffic().isEmpty());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void clientIpMigrationPreservesExistingRules() throws Exception {
        var rules = store.trafficRules();
        var mac = TrafficRule.create(null, "mac", "1a:11:12:13:14:15", "旧设备");
        rules.insert(mac);
        var vpn = TrafficRule.create(null, "client_ip", "10.10.0.2", "vpn");
        try (var connection = database.connect(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE r86s_traffic_rule MODIFY (rule_kind VARCHAR2(8 CHAR))");
            assertTrue(TrafficRuleStore.legacyKindConstraint(assertThrows(SQLException.class, () -> rules.insert(vpn))));
            for (String command : Files.readString(Path.of("sql/oracle-client-ip-rules.sql")).split(";")) {
                if (!command.isBlank()) statement.execute(command);
            }
        }
        rules.insert(vpn);
        assertTrue(rules.list().containsAll(List.of(mac, vpn)));
    }

    private static Map<String, Object> clientFlow(String scope, String region, String route, String client, String mac,
                                                  String target, long rx, long tx) {
        return Map.of("scope", scope, "region", region, "route", route, "client_ip", client, "client_mac", mac,
                "target_ip", target, "target_port", 443, "protocol", "tcp", "rx_bytes", rx, "tx_bytes", tx);
    }

    @Test
    void httpPreservesStatusAndServesHistory() throws Exception {
        var latest = new HashMap<>(sample());
        latest.put("load", List.of(1.5, 1.1, 0.7));
        latest.put("memory", Map.of("MemAvailable", 1000L, "SwapTotal", 4096L, "SwapFree", 1024L));
        var highLoad = new HashMap<>(latest);
        highLoad.put("sample_id", "d".repeat(32));
        highLoad.put("load", List.of(2.5, 1.2, 0.8));
        highLoad.put("memory", Map.of("SwapTotal", 0L, "SwapFree", 0L));
        latest.put("client_traffic", Map.of("state", "ok", "flows", List.of(
                clientFlow("internet", "cn", "direct", "192.168.1.2", "10:11:12:13:14:15", "203.0.113.1", 10, 2))));
        batch(FIRST, List.of(event(EVENT_ONE, "kernel", "out of memory")), List.of(latest, highLoad), true);
        importer(false).importOnce();
        latest.put("sampled_at", Instant.now().toString());
        latest.put("wan", Map.of("up", true, "l3_device", "pppoe-wan"));
        latest.put("disks", Map.of("/fourT", Map.of("total_bytes", 10000, "used_bytes", 1200, "free_bytes", 8000,
                "filesystem_id", "8:17", "device", "/dev/sdb1", "mount_root", "/segments")));
        latest.put("network", Map.of("eth2", Map.of("operstate", "up", "carrier", true, "speed_mbps", 2500),
                "pppoe-wan", Map.of("operstate", "up"), "pppoe-IPTV", Map.of("operstate", "up"),
                "xray:direct", Map.of("kind", "xray", "operstate", "up"),
                "forwarded:wan", Map.of("kind", "forwarded", "iptv_excluded", true),
                "wg0", Map.of("operstate", "unknown", "last_handshake_at", time.toString())));
        latest.put("directories", List.of(Map.of("path", "/fourT/openwrt-monitor", "used_bytes", 450L,
                "measured_at", time.toString())));
        Files.writeString(root.resolve("state/latest-sample.json"), JSON.writeValueAsString(latest));
        StatusReader status = new StatusReader(root.resolve("state"), JSON);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/", new MonitorApi(JSON, status, store)::handle);
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/api/v1/";
            var statusResponse = get(client, base + "status");
            JsonNode state = JSON.readTree(statusResponse.body());
            assertEquals(200, statusResponse.statusCode());
            assertEquals("fresh", state.path("sampling").path("state").asText());
            assertEquals("pppoe-wan", state.path("wan").path("l3Device").asText());
            assertEquals("missing", state.path("awsUpload").path("state").asText());
            JsonNode live = state.path("liveMetrics");
            assertEquals(32.5, live.path("cpuPercent").asDouble());
            assertEquals(1.5, live.path("load1").asDouble());
            assertEquals(1.1, live.path("load5").asDouble());
            assertEquals(0.7, live.path("load15").asDouble());
            assertEquals(3072L, live.path("swapUsedBytes").asLong());
            assertEquals("8:17", live.path("filesystems").get(0).path("filesystemId").asText());
            assertEquals("/segments", live.path("filesystems").get(0).path("mountRoot").asText());
            assertEquals(8000L, live.path("filesystems").get(0).path("availableBytes").asLong());
            assertEquals(time.toString(), live.path("directories").get(0).path("measuredAt").asText());
            String range = "?from=" + time.minusSeconds(1) + "&to=" + time.plusSeconds(1);
            JsonNode metricWindow = JSON.readTree(get(client, base + "metrics" + range).body()).path("windows").get(0);
            assertEquals(2, metricWindow.path("sampleCount").asInt());
            assertEquals(2.5, metricWindow.path("load1Max").asDouble());
            assertEquals(3072L, metricWindow.path("swapUsedMaxBytes").asLong());
            assertEquals(4096L, metricWindow.path("swapTotalMaxBytes").asLong());
            assertEquals(EVENT_ONE, JSON.readTree(get(client, base + "events" + range + "&type=oom").body())
                    .path("items").get(0).path("id").asText());
            assertEquals("out of memory", JSON.readTree(get(client, base + "events/" + EVENT_ONE).body())
                    .path("raw").asText());
            assertEquals(400, get(client, base + "events?limit=201").statusCode());
            assertEquals(404, get(client, base + "events/" + "f".repeat(64)).statusCode());

            var network = JSON.readTree(get(client, base + "network").body());
            assertEquals("fresh", network.path("state").asText());
            assertEquals("pppoe-wan", network.path("internetInterface").asText());
            var ethernet = findInterface(network, "eth2");
            assertEquals(2500, ethernet.path("speedMbps").asInt());
            assertFalse(ethernet.path("trafficEligible").asBoolean());
            assertTrue(ethernet.path("mayIncludeIptv").asBoolean());
            assertTrue(findInterface(network, "pppoe-IPTV").path("iptv").asBoolean());
            assertEquals(time.toString(), findInterface(network, "wg0").path("lastHandshakeAt").asText());
            assertTrue(findInterface(network, "forwarded:wan").path("trafficEligible").asBoolean());
            assertFalse(findInterface(network, "forwarded:wan").path("mayIncludeIptv").asBoolean());
            String traffic = base + "network/traffic" + range + "&interface=";
            assertEquals(400, get(client, traffic + "pppoe-IPTV").statusCode());
            assertEquals(400, get(client, traffic + "eth0%22").statusCode());
            assertEquals("xray:direct", JSON.readTree(get(client, traffic + "xray:direct").body())
                    .path("interface").asText());
            assertFalse(JSON.readTree(get(client, traffic + "pppoe-wan").body()).path("mayIncludeIptv").asBoolean());
            assertTrue(JSON.readTree(get(client, traffic + "eth2").body()).path("mayIncludeIptv").asBoolean());

            String clientTraffic = base + "client-traffic" + range;
            String token = "a".repeat(64);
            assertEquals(401, get(client, clientTraffic, token).statusCode());
            Files.writeString(root.resolve("state/client-traffic.token"), token + "\n");
            assertEquals(401, get(client, clientTraffic).statusCode());
            assertEquals(401, get(client, clientTraffic, "b".repeat(64)).statusCode());
            var clients = get(client, clientTraffic + "&scope=internet&region=cn&client=10%3A11%3A12%3A13%3A14%3A15", token);
            assertEquals(200, clients.statusCode());
            var clientPayload = JSON.readTree(clients.body());
            assertEquals("available", clientPayload.path("state").asText());
            assertEquals(300, clientPayload.path("windowSeconds").asInt());
            assertEquals(10, clientPayload.path("windows").get(0).path("rxBytes").asInt());
            assertEquals("10:11:12:13:14:15", clientPayload.path("devices").get(0).path("id").asText());
            assertEquals(400, get(client, clientTraffic + "&region=xyz", token).statusCode());
            assertEquals(400, get(client, clientTraffic + "&scope=internal&region=cn", token).statusCode());
            assertEquals(400, get(client, base + "client-traffic?from=" + time.minus(Duration.ofDays(61))
                    + "&to=" + time, token).statusCode());

            String bandwidth = base + "network/bandwidth?interface=";
            assertEquals(400, get(client, bandwidth + "pppoe-IPTV").statusCode());
            assertEquals(503, get(client, bandwidth + "pppoe-wan").statusCode());
            Instant fresh = Instant.now();
            var realtime = new HashMap<String, Object>();
            realtime.put("sampledAt", fresh.toString());
            realtime.put("interfaces", Map.of("pppoe-wan", Map.of(
                    "interfaceName", "pppoe-wan", "from", fresh.minusSeconds(3).toString(), "to", fresh.toString(),
                    "windowSeconds", 3, "scope", "luci", "mayIncludeIptv", false,
                    "windows", List.of(Map.of("start", fresh.minusSeconds(3).toString(), "rxBytes", 53, "txBytes", 15,
                            "sampleCount", 1, "durationSeconds", 3)))));
            Path latestBandwidth = root.resolve("state/latest-bandwidth.json");
            JSON.writeValue(latestBandwidth.toFile(), realtime);
            var series = JSON.readTree(get(client, bandwidth + "pppoe-wan").body());
            assertEquals("luci", series.path("scope").asText());
            assertEquals(53, series.path("windows").get(0).path("rxBytes").asInt());
            assertEquals(503, get(client, bandwidth + "eth2").statusCode());

            server.removeContext("/api/v1/");
            server.createContext("/api/v1/", new MonitorApi(JSON, status, null)::handle);
            assertEquals(503, get(client, base + "metrics").statusCode());
            assertEquals(200, get(client, base + "status").statusCode());
            assertEquals(200, get(client, base + "network").statusCode());
            assertEquals(503, get(client, traffic + "pppoe-wan").statusCode());
            assertEquals(503, get(client, clientTraffic, token).statusCode());
            assertEquals(200, get(client, bandwidth + "pppoe-wan").statusCode());
            realtime.put("sampledAt", fresh.minusSeconds(21).toString());
            JSON.writeValue(latestBandwidth.toFile(), realtime);
            assertEquals(503, get(client, bandwidth + "pppoe-wan").statusCode());
        } finally {
            server.stop(0);
        }
    }

    private static JsonNode findInterface(JsonNode response, String name) {
        for (JsonNode link : response.path("interfaces")) {
            if (name.equals(link.path("name").asText())) {
                return link;
            }
        }
        throw new AssertionError("Missing interface " + name);
    }

    private Map<String, Object> trafficSample(Instant sampledAt, int id, String boot, String counter, long rx, long tx) {
        var record = new HashMap<>(sample());
        record.put("sample_id", "%032x".formatted(id));
        record.put("sampled_at", sampledAt.toString());
        record.put("boot_id", boot);
        record.put("network", Map.of("pppoe-wan", Map.of("rx_bytes", rx, "tx_bytes", tx, "counter_id", counter),
                "forwarded:wan", Map.of("rx_bytes", rx, "tx_bytes", tx, "kind", "forwarded")));
        return record;
    }

    private HistoryImporter importer(boolean cleanup) {
        var config = new AppConfig(root, "127.0.0.1", 0, "r86s", 30, 180, cleanup, database);
        return new HistoryImporter(config, JSON, store);
    }

    private Map<String, Object> event(String id, String category, String raw) {
        return Map.of("host_id", "r86s", "event_id", id, "observed_at", time.toString(),
                "category", category, "severity", "info", "raw", raw, "source", "fixture");
    }

    private Map<String, Object> sample() {
        return Map.of("host_id", "r86s", "sample_id", SAMPLE, "sampled_at", time.toString(),
                "cpu_percent", 32.5, "network", Map.of("eth2", Map.of("operstate", "down")),
                "errors", Map.of());
    }

    private Path closedRaw(String category, Map<String, Object> event) throws IOException {
        Path directory = root.resolve("raw").resolve(time.atOffset(ZoneOffset.UTC).toLocalDate().toString());
        Files.createDirectories(directory);
        Path file = directory.resolve(category + ".jsonl");
        Files.write(file, jsonl(List.of(event)));
        Files.setLastModifiedTime(file, FileTime.from(time));
        return file;
    }

    private Path batch(String id, List<Map<String, Object>> events, List<Map<String, Object>> metrics,
                       boolean confirmed) throws Exception {
        byte[] logData = jsonl(events);
        byte[] metricData = jsonl(metrics);
        byte[] summary = JSON.writeValueAsBytes(Map.of("batch_id", id));
        byte[] manifest = JSON.writeValueAsBytes(Map.of("batch_id", id, "host_id", "r86s", "format_version", 1,
                "sealed_at", time.plusSeconds(1).toString(), "counts",
                Map.of("events.jsonl", events.size(), "metrics.jsonl", metrics.size()),
                "files", Map.of("events.jsonl", fileEntry(logData), "metrics.jsonl", fileEntry(metricData),
                        "summary.json", fileEntry(summary))));
        Path file = root.resolve("batches/sent").resolve(id + ".tar.gz");
        try (var tar = new TarArchiveOutputStream(new GZIPOutputStream(Files.newOutputStream(file)))) {
            put(tar, "events.jsonl", logData);
            put(tar, "metrics.jsonl", metricData);
            put(tar, "summary.json", summary);
            put(tar, "manifest.json", manifest);
        }
        if (confirmed) {
            JSON.writeValue(root.resolve("state").resolve(id + ".ack.json").toFile(),
                    Map.of("batch_id", id, "sha256", digest(Files.readAllBytes(file)), "status", "accepted"));
        }
        return file;
    }

    private static void put(TarArchiveOutputStream tar, String name, byte[] bytes) throws IOException {
        var member = new TarArchiveEntry(name);
        member.setSize(bytes.length);
        member.addPaxHeader("mtime", "1700000000.123456");
        tar.putArchiveEntry(member);
        tar.write(bytes);
        tar.closeArchiveEntry();
    }

    private static byte[] jsonl(List<Map<String, Object>> rows) throws IOException {
        var output = new StringBuilder();
        for (var row : rows) {
            output.append(JSON.writeValueAsString(row)).append('\n');
        }
        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, Object> fileEntry(byte[] bytes) throws Exception {
        return Map.of("bytes", bytes.length, "sha256", digest(bytes));
    }

    private static String digest(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private int rows(String table) throws SQLException {
        try (var connection = database.connect(); var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            result.next();
            return result.getInt(1);
        }
    }

    private void execute(String sql) throws SQLException {
        try (var connection = database.connect(); var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static HttpResponse<String> get(HttpClient client, String url) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(HttpClient client, String url, String token) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).header("Authorization", "Bearer " + token).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> sendRule(HttpClient client, String url, String token, String method, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json");
        if (token != null) request.header("Authorization", "Bearer " + token);
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}
