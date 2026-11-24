package org.example.r86s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.r86s.BatchArchive.Batch;
import org.example.r86s.HistoryData.Event;
import org.example.r86s.HistoryData.EventCursor;
import org.example.r86s.HistoryData.EventDetail;
import org.example.r86s.HistoryData.EventSummary;
import org.example.r86s.HistoryData.Metric;
import org.example.r86s.HistoryData.Window;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

final class HistoryStore {
    private static final int JDBC_GROUP_SIZE = 256;
    private static final int DIRECT_JSON_BYTES = 3900;
    private static final String INSERT_METRIC = """
            MERGE INTO r86s_metric_sample d
            USING (SELECT CAST(? AS VARCHAR2(128)) host_id, CAST(? AS VARCHAR2(32)) sample_id FROM dual) s
            ON (d.host_id = s.host_id AND d.sample_id = s.sample_id)
            WHEN NOT MATCHED THEN INSERT (
                host_id, sample_id, sampled_ms, window_ms, cpu_pct, iowait_pct, mem_avail,
                temperature, overlay_free, docker_free, archive_free, disk_await, conntrack_pct,
                wan_up, eth0_up, eth2_up, has_errors, record_json
            ) VALUES (s.host_id, s.sample_id, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    private static final String INSERT_EVENT = """
            MERGE INTO r86s_event d
            USING (SELECT CAST(? AS VARCHAR2(128)) host_id, CAST(? AS VARCHAR2(64)) event_id FROM dual) s
            ON (d.host_id = s.host_id AND d.event_id = s.event_id)
            WHEN NOT MATCHED THEN INSERT (
                host_id, event_id, observed_ms, category, event_type, severity, log_source, preview, record_json
            ) VALUES (s.host_id, s.event_id, ?, ?, ?, ?, ?, ?, ?)
            """;
    private static final String WINDOWS = """
            WITH samples AS (
                SELECT window_ms, has_errors, cpu_pct, iowait_pct, mem_avail, temperature,
                    overlay_free, docker_free, archive_free, disk_await, conntrack_pct, wan_up, eth0_up, eth2_up,
                    JSON_VALUE(record_json, '$.load[0]'%1$s) load1,
                    JSON_VALUE(record_json, '$.memory.SwapTotal'%1$s) swap_total,
                    JSON_VALUE(record_json, '$.memory.SwapFree'%1$s) swap_free
                FROM r86s_metric_sample
                WHERE host_id = ? AND sampled_ms >= ? AND sampled_ms < ?
            )
            SELECT window_ms, COUNT(*) sample_count, SUM(has_errors) error_samples,
                AVG(cpu_pct) cpu_avg, MAX(cpu_pct) cpu_max, MAX(iowait_pct) iowait_max,
                MIN(mem_avail) mem_min, MAX(temperature) temp_max, MIN(overlay_free) overlay_min,
                MIN(docker_free) docker_min, MIN(archive_free) archive_min, MAX(disk_await) await_max,
                MAX(conntrack_pct) conntrack_max, SUM(1 - wan_up) wan_down,
                SUM(1 - eth0_up) eth0_down, SUM(1 - eth2_up) eth2_down,
                MAX(CASE WHEN load1 >= 0 THEN load1 END) load1_max,
                MAX(CASE WHEN swap_free BETWEEN 0 AND swap_total THEN swap_total - swap_free END) swap_used_max,
                MAX(CASE WHEN swap_total >= 0 THEN swap_total END) swap_total_max
            FROM samples
            GROUP BY window_ms
            """;
    private static final String EVENT_COLUMNS = "event_id, observed_ms, category, event_type, severity, log_source, preview";

    private final AppConfig.Database database;
    private final String host;
    private final ObjectMapper json;
    private final String windowsSql;
    private final Path state;
    private final TrafficRuleStore trafficRules;

    HistoryStore(AppConfig.Database database, String host, ObjectMapper json) {
        this(database, host, json, (Path) null);
    }

    HistoryStore(AppConfig.Database database, String host, ObjectMapper json, Path state) {
        this(database, host, json, state, " RETURNING NUMBER NULL ON ERROR");
    }

    // H2 integration tests supply a numeric JSON_VALUE alias for the Oracle-only RETURNING clause.
    HistoryStore(AppConfig.Database database, String host, ObjectMapper json, String jsonNumberClause) {
        this(database, host, json, null, jsonNumberClause);
    }

    HistoryStore(AppConfig.Database database, String host, ObjectMapper json, Path state, String jsonNumberClause) {
        this.database = database;
        this.host = host;
        this.json = json;
        this.state = state;
        this.trafficRules = new TrafficRuleStore(database, host);
        this.windowsSql = WINDOWS.formatted(jsonNumberClause);
    }

    Connection connect() throws SQLException {
        return database.connect();
    }

    TrafficRuleStore trafficRules() {
        return trafficRules;
    }

    Map<String, String> importedBatches(Connection connection, String firstId) throws SQLException {
        Map<String, String> batches = new HashMap<>();
        try (PreparedStatement sql = connection.prepareStatement("""
                SELECT batch_id, sha256 FROM r86s_ingest_batch WHERE host_id = ? AND batch_id >= ?
                """)) {
            sql.setString(1, host);
            sql.setString(2, firstId);
            try (ResultSet rows = sql.executeQuery()) {
                while (rows.next()) {
                    batches.put(rows.getString(1), rows.getString(2));
                }
            }
        }
        return batches;
    }

    void writeBatch(Connection connection, Batch batch) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new SQLException("Archive writes require a transaction");
        }
        try {
            if (!batch.metrics().isEmpty()) {
                writeMetrics(connection, batch.metrics());
            }
            if (!batch.events().isEmpty()) {
                writeEvents(connection, batch.events());
            }
            try (PreparedStatement sql = connection.prepareStatement("""
                    INSERT INTO r86s_ingest_batch
                        (host_id, batch_id, sha256, sealed_ms, imported_ms, event_count, sample_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """)) {
                sql.setString(1, host);
                sql.setString(2, batch.id());
                sql.setString(3, batch.checksum());
                sql.setLong(4, batch.sealedMs());
                sql.setLong(5, System.currentTimeMillis());
                sql.setInt(6, batch.events().size());
                sql.setInt(7, batch.metrics().size());
                sql.executeUpdate();
            }
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, e);
            throw e;
        }
    }

    boolean containsAll(Connection connection, boolean metrics, Set<String> ids) throws SQLException {
        String table = metrics ? "r86s_metric_sample" : "r86s_event";
        String key = metrics ? "sample_id" : "event_id";
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String query = "SELECT " + key + " FROM " + table + " WHERE host_id = ? AND " + key
                + " IN (" + placeholders + ")";
        Set<String> missing = new HashSet<>(ids);
        try (PreparedStatement sql = connection.prepareStatement(query)) {
            sql.setString(1, host);
            int i = 2;
            for (String id : ids) {
                sql.setString(i++, id);
            }
            try (ResultSet rows = sql.executeQuery()) {
                while (rows.next()) {
                    missing.remove(rows.getString(1));
                }
            }
        }
        return missing.isEmpty();
    }

    void expireHistory(Connection connection, long cutoff) throws SQLException {
        try {
            deleteOlder(connection, "DELETE FROM r86s_metric_sample WHERE host_id = ? AND sampled_ms < ?", cutoff);
            deleteOlder(connection, "DELETE FROM r86s_event WHERE host_id = ? AND observed_ms < ?", cutoff);
            deleteOlder(connection, "DELETE FROM r86s_ingest_batch WHERE host_id = ? AND sealed_ms < ?", cutoff);
            connection.commit();
        } catch (SQLException e) {
            rollback(connection, e);
            throw e;
        }
    }

    private void deleteOlder(Connection connection, String query, long cutoff) throws SQLException {
        try (PreparedStatement sql = connection.prepareStatement(query)) {
            sql.setString(1, host);
            sql.setLong(2, cutoff);
            sql.executeUpdate();
        }
    }

    private static void rollback(Connection connection, SQLException error) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            error.addSuppressed(e);
        }
    }

    private void writeMetrics(Connection connection, List<Metric> metrics) throws SQLException {
        writeRows(connection, INSERT_METRIC, metrics, 18, Metric::recordJson, (sql, row) -> {
            sql.setString(2, row.id());
            sql.setLong(3, row.time());
            sql.setLong(4, HistoryData.windowStart(row.time()));
            numbers(sql, 5, row.cpu(), row.ioWait(), row.memoryAvailable(), row.temperature(),
                    row.overlayFree(), row.dockerFree(), row.archiveFree(), row.diskAwait(),
                    row.conntrackPercent(), bit(row.wanUp()), bit(row.eth0Up()), bit(row.eth2Up()),
                    row.hasErrors() ? 1 : 0);
        });
    }

    private void writeEvents(Connection connection, List<Event> events) throws SQLException {
        writeRows(connection, INSERT_EVENT, events, 9, Event::recordJson, (sql, row) -> {
            sql.setString(2, row.id());
            sql.setLong(3, row.time());
            sql.setString(4, row.category());
            sql.setString(5, row.type());
            sql.setString(6, row.severity());
            sql.setString(7, row.source());
            sql.setString(8, row.preview());
        });
    }

    private <T> void writeRows(Connection connection, String query, List<T> rows, int jsonIndex,
                               Function<T, String> recordJson, RowBinder<T> binder) throws SQLException {
        try (PreparedStatement grouped = connection.prepareStatement(query);
             PreparedStatement large = connection.prepareStatement(query)) {
            int pending = 0;
            grouped.setString(1, host);
            large.setString(1, host);
            for (T row : rows) {
                String payload = recordJson.apply(row);
                if (fitsDirectBind(payload)) {
                    binder.bind(grouped, row);
                    grouped.setString(jsonIndex, payload);
                    grouped.addBatch();
                    pending++;
                    if (pending == JDBC_GROUP_SIZE) {
                        flushRows(grouped, pending);
                        pending = 0;
                    }
                } else {
                    // Preserve row order when the same ID appears on either side of a large record.
                    flushRows(grouped, pending);
                    pending = 0;
                    binder.bind(large, row);
                    large.setClob(jsonIndex, new StringReader(payload), payload.length());
                    large.executeUpdate();
                }
            }
            flushRows(grouped, pending);
        }
    }

    private static void flushRows(PreparedStatement statement, int pending) throws SQLException {
        if (pending == 0) {
            return;
        }
        int[] counts = statement.executeBatch();
        if (counts.length != pending) {
            throw new SQLException("Incomplete JDBC update count for archive rows");
        }
        for (int count : counts) {
            if (count == Statement.EXECUTE_FAILED) {
                throw new SQLException("A row failed in the JDBC group");
            }
        }
    }

    private static boolean fitsDirectBind(String json) {
        if (json.length() > DIRECT_JSON_BYTES) {
            return false;
        }
        int bytes = 0;
        for (int i = 0; i < json.length(); i++) {
            char ch = json.charAt(i);
            // Count surrogate halves separately to cover the legacy Oracle UTF8 encoding too.
            bytes += ch < 0x80 ? 1 : ch < 0x800 ? 2 : 3;
            if (bytes > DIRECT_JSON_BYTES) {
                return false;
            }
        }
        return true;
    }

    @FunctionalInterface
    private interface RowBinder<T> {
        void bind(PreparedStatement statement, T row) throws SQLException;
    }

    List<Window> windows(long from, long to) throws SQLException {
        Map<Long, Window> found = new HashMap<>();
        try (Connection connection = connect(); PreparedStatement sql = connection.prepareStatement(windowsSql)) {
            sql.setString(1, host);
            sql.setLong(2, from);
            sql.setLong(3, to);
            try (ResultSet rows = sql.executeQuery()) {
                while (rows.next()) {
                    long start = rows.getLong("window_ms");
                    found.put(start, new Window(HistoryData.iso(start), rows.getInt("sample_count"),
                            rows.getInt("error_samples"), decimal(rows, "cpu_avg"), decimal(rows, "cpu_max"),
                            decimal(rows, "iowait_max"), whole(rows, "mem_min"), decimal(rows, "temp_max"),
                            whole(rows, "overlay_min"), whole(rows, "docker_min"), whole(rows, "archive_min"),
                            decimal(rows, "await_max"), decimal(rows, "conntrack_max"), count(rows, "wan_down"),
                            count(rows, "eth0_down"), count(rows, "eth2_down"),
                            decimal(rows, "load1_max"), whole(rows, "swap_used_max"), whole(rows, "swap_total_max")));
                }
            }
        }
        List<Window> windows = new ArrayList<>();
        for (long start = HistoryData.windowStart(from); start < to; start += HistoryData.WINDOW_MS) {
            Window window = found.get(start);
            windows.add(window == null ? Window.empty(start) : window);
        }
        return windows;
    }

    List<NetworkData.TrafficWindow> traffic(long from, long to, String name) throws SQLException {
        if (!NetworkData.validName(name)) {
            throw new IllegalArgumentException("Invalid interface name");
        }
        var traffic = new NetworkData.TrafficAccumulator(from, to);
        try (Connection connection = connect()) {
            boolean oracle = "Oracle".equals(connection.getMetaData().getDatabaseProductName());
            // Oracle JSON paths are literals. The shared interface validator excludes quotes and path syntax.
            String projection = oracle
                    ? "JSON_VALUE(record_json, '$.boot_id'), JSON_QUERY(record_json, '$.network.\"" + name + "\"')"
                    : "record_json";
            String query = "SELECT sampled_ms, " + projection
                    + " FROM r86s_metric_sample WHERE host_id = ? AND sampled_ms >= ? AND sampled_ms < ?"
                    + " ORDER BY sampled_ms, sample_id";
            try (PreparedStatement sql = connection.prepareStatement(query)) {
                sql.setString(1, host);
                sql.setLong(2, from);
                sql.setLong(3, to);
                sql.setFetchSize(128);
                try (ResultSet rows = sql.executeQuery()) {
                    while (rows.next()) {
                        long at = rows.getLong(1);
                        if (oracle) {
                            String counters = rows.getString(3);
                            traffic.accept(counters == null ? null
                                    : NetworkData.counter(name, at, json.readTree(counters), rows.getString(2)));
                        } else {
                            JsonNode document = json.readTree(rows.getString(2));
                            traffic.accept(NetworkData.counter(name, at, document.path("network").path(name),
                                    document.path("boot_id").textValue()));
                        }
                    }
                } catch (IOException e) {
                    throw new SQLException("Invalid archived network counters", e);
                }
            }
        }
        return traffic.windows();
    }

    ClientTraffic.Response clientTraffic(long from, long to, ClientTraffic.Filters filters) throws SQLException {
        var traffic = new ClientTraffic.Accumulator(from, to, filters, new TrafficServices(state, trafficRules.forTraffic()));
        readClientTraffic(from, to, false, (time, sample, proxy) -> traffic.accept(time, sample));
        return traffic.response();
    }

    ClientTraffic.AwsUsage awsUsage(long from, long to) throws SQLException {
        var traffic = new ClientTraffic.AwsAccumulator(from, to);
        readClientTraffic(from, to, true, traffic::accept);
        return traffic.result();
    }

    private void readClientTraffic(long from, long to, boolean includeProxy, ClientTrafficSample consumer) throws SQLException {
        try (Connection connection = connect()) {
            boolean oracle = "Oracle".equals(connection.getMetaData().getDatabaseProductName());
            String projection = oracle ? "JSON_QUERY(record_json, '$.client_traffic' RETURNING CLOB)" : "record_json";
            if (oracle && includeProxy) projection += ", JSON_QUERY(record_json, '$.proxy_traffic' RETURNING CLOB)";
            String query = "SELECT sampled_ms, " + projection
                    + " FROM r86s_metric_sample WHERE host_id = ? AND sampled_ms >= ? AND sampled_ms < ?"
                    + " ORDER BY sampled_ms, sample_id";
            try (PreparedStatement sql = connection.prepareStatement(query)) {
                sql.setString(1, host);
                sql.setLong(2, from);
                sql.setLong(3, to);
                sql.setFetchSize(64);
                try (ResultSet rows = sql.executeQuery()) {
                    while (rows.next()) {
                        long at = rows.getLong(1);
                        JsonNode document = archivedJson(rows, 2);
                        if (oracle) {
                            consumer.accept(at, document, includeProxy ? archivedJson(rows, 3) : null);
                        } else if (document != null) {
                            consumer.accept(at, document.path("client_traffic"), document.path("proxy_traffic"));
                        }
                    }
                }
            }
        } catch (IOException | ArithmeticException e) {
            throw new SQLException("Invalid archived client traffic", e);
        }
    }

    private JsonNode archivedJson(ResultSet rows, int column) throws SQLException, IOException {
        try (var reader = rows.getCharacterStream(column)) {
            return reader == null ? null : json.readTree(reader);
        }
    }

    @FunctionalInterface
    private interface ClientTrafficSample {
        void accept(long time, JsonNode sample, JsonNode proxy);
    }

    EventResults events(long from, long to, String type, EventCursor cursor, int limit, boolean all) throws SQLException {
        String query = "SELECT " + EVENT_COLUMNS + " FROM r86s_event WHERE host_id = ? AND observed_ms >= ? AND observed_ms < ?"
                + (type == null ? (all ? "" : " AND event_type <> 'other'") : " AND event_type = ?")
                + (cursor == null ? "" : " AND (observed_ms < ? OR (observed_ms = ? AND event_id < ?))")
                + " ORDER BY observed_ms DESC, event_id DESC FETCH FIRST ? ROWS ONLY";
        List<EventSummary> items = new ArrayList<>();
        EventCursor next = null;
        try (Connection connection = connect(); PreparedStatement sql = connection.prepareStatement(query)) {
            int i = 1;
            sql.setString(i++, host);
            sql.setLong(i++, from);
            sql.setLong(i++, to);
            if (type != null) {
                sql.setString(i++, type);
            }
            if (cursor != null) {
                sql.setLong(i++, cursor.time());
                sql.setLong(i++, cursor.time());
                sql.setString(i++, cursor.id());
            }
            sql.setInt(i, limit + 1);
            try (ResultSet rows = sql.executeQuery()) {
                while (rows.next()) {
                    if (items.size() == limit) {
                        return new EventResults(items, next);
                    }
                    items.add(eventSummary(rows));
                    next = new EventCursor(rows.getLong("observed_ms"), rows.getString("event_id"));
                }
            }
        }
        return new EventResults(items, null);
    }

    EventDetail event(String id) throws SQLException, IOException {
        String query = "SELECT " + EVENT_COLUMNS + ", record_json FROM r86s_event WHERE host_id = ? AND event_id = ?";
        try (Connection connection = connect(); PreparedStatement sql = connection.prepareStatement(query)) {
            sql.setString(1, host);
            sql.setString(2, id);
            try (ResultSet row = sql.executeQuery()) {
                if (!row.next()) {
                    return null;
                }
                JsonNode record = json.readTree(row.getString("record_json"));
                String raw = record.path("raw").textValue();
                if (raw == null) {
                    raw = record.path("snapshot").toString();
                }
                return new EventDetail(eventSummary(row), raw, record);
            }
        }
    }

    private static EventSummary eventSummary(ResultSet row) throws SQLException {
        return new EventSummary(row.getString("event_id"), HistoryData.iso(row.getLong("observed_ms")),
                row.getString("category"), row.getString("event_type"), row.getString("severity"),
                row.getString("log_source"), row.getString("preview"));
    }

    private static void numbers(PreparedStatement sql, int first, Number... values) throws SQLException {
        for (Number value : values) {
            sql.setObject(first++, value, Types.NUMERIC);
        }
    }

    private static Integer bit(Boolean bool) {
        return bool == null ? null : (bool ? 1 : 0);
    }

    private static Double decimal(ResultSet rows, String key) throws SQLException {
        double value = rows.getDouble(key);
        return rows.wasNull() ? null : value;
    }

    private static Long whole(ResultSet rows, String key) throws SQLException {
        long value = rows.getLong(key);
        return rows.wasNull() ? null : value;
    }

    private static Integer count(ResultSet rows, String key) throws SQLException {
        int value = rows.getInt(key);
        return rows.wasNull() ? null : value;
    }

    record EventResults(List<EventSummary> items, EventCursor next) {
    }
}
