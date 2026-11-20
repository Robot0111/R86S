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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class HistoryStore {
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
            SELECT window_ms, COUNT(*) sample_count, SUM(has_errors) error_samples,
                AVG(cpu_pct) cpu_avg, MAX(cpu_pct) cpu_max, MAX(iowait_pct) iowait_max,
                MIN(mem_avail) mem_min, MAX(temperature) temp_max, MIN(overlay_free) overlay_min,
                MIN(docker_free) docker_min, MIN(archive_free) archive_min, MAX(disk_await) await_max,
                MAX(conntrack_pct) conntrack_max, SUM(1 - wan_up) wan_down,
                SUM(1 - eth0_up) eth0_down, SUM(1 - eth2_up) eth2_down
            FROM r86s_metric_sample
            WHERE host_id = ? AND sampled_ms >= ? AND sampled_ms < ?
            GROUP BY window_ms
            """;
    private static final String EVENT_COLUMNS = "event_id, observed_ms, category, event_type, severity, log_source, preview";

    private final AppConfig.Database database;
    private final String host;
    private final ObjectMapper json;

    HistoryStore(AppConfig.Database database, String host, ObjectMapper json) {
        this.database = database;
        this.host = host;
        this.json = json;
    }

    Connection connect() throws SQLException {
        return database.connect();
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
        try (PreparedStatement sql = connection.prepareStatement(INSERT_METRIC)) {
            sql.setString(1, host);
            for (Metric row : metrics) {
                sql.setString(2, row.id());
                sql.setLong(3, row.time());
                sql.setLong(4, HistoryData.windowStart(row.time()));
                numbers(sql, 5, row.cpu(), row.ioWait(), row.memoryAvailable(), row.temperature(),
                        row.overlayFree(), row.dockerFree(), row.archiveFree(), row.diskAwait(),
                        row.conntrackPercent(), bit(row.wanUp()), bit(row.eth0Up()), bit(row.eth2Up()),
                        row.hasErrors() ? 1 : 0);
                sql.setClob(18, new StringReader(row.recordJson()), row.recordJson().length());
                sql.executeUpdate();
            }
        }
    }

    private void writeEvents(Connection connection, List<Event> events) throws SQLException {
        try (PreparedStatement sql = connection.prepareStatement(INSERT_EVENT)) {
            sql.setString(1, host);
            for (Event row : events) {
                sql.setString(2, row.id());
                sql.setLong(3, row.time());
                sql.setString(4, row.category());
                sql.setString(5, row.type());
                sql.setString(6, row.severity());
                sql.setString(7, row.source());
                sql.setString(8, row.preview());
                sql.setClob(9, new StringReader(row.recordJson()), row.recordJson().length());
                sql.executeUpdate();
            }
        }
    }

    List<Window> windows(long from, long to) throws SQLException {
        Map<Long, Window> found = new HashMap<>();
        try (Connection connection = connect(); PreparedStatement sql = connection.prepareStatement(WINDOWS)) {
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
                            count(rows, "eth0_down"), count(rows, "eth2_down")));
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
