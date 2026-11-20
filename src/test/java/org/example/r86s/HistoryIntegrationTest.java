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

class HistoryIntegrationTest {
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
        store = new HistoryStore(database, "r86s", JSON);
        time = Instant.now().minus(Duration.ofDays(2)).truncatedTo(java.time.temporal.ChronoUnit.MINUTES).plusSeconds(15);
        try (var connection = database.connect(); var statement = connection.createStatement()) {
            for (String command : Files.readString(Path.of("sql/oracle-schema.sql")).split(";")) {
                if (!command.isBlank()) {
                    statement.execute(command);
                }
            }
        }
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
    void httpPreservesStatusAndServesHistory() throws Exception {
        batch(FIRST, List.of(event(EVENT_ONE, "kernel", "out of memory")), List.of(sample()), true);
        importer(false).importOnce();
        Files.writeString(root.resolve("state/latest-sample.json"), JSON.writeValueAsString(Map.of(
                "sampled_at", Instant.now().toString(),
                "wan", Map.of("up", true, "l3_device", "pppoe-wan"))));
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
            String range = "?from=" + time.minusSeconds(1) + "&to=" + time.plusSeconds(1);
            assertEquals(1, JSON.readTree(get(client, base + "metrics" + range).body())
                    .path("windows").get(0).path("sampleCount").asInt());
            assertEquals(EVENT_ONE, JSON.readTree(get(client, base + "events" + range + "&type=oom").body())
                    .path("items").get(0).path("id").asText());
            assertEquals("out of memory", JSON.readTree(get(client, base + "events/" + EVENT_ONE).body())
                    .path("raw").asText());
            assertEquals(400, get(client, base + "events?limit=201").statusCode());
            assertEquals(404, get(client, base + "events/" + "f".repeat(64)).statusCode());

            server.removeContext("/api/v1/");
            server.createContext("/api/v1/", new MonitorApi(JSON, status, null)::handle);
            assertEquals(503, get(client, base + "metrics").statusCode());
            assertEquals(200, get(client, base + "status").statusCode());
        } finally {
            server.stop(0);
        }
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
}
