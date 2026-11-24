package org.example.r86s;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

final class HistoryData {
    static final long WINDOW_MS = 300_000;
    static final Pattern EVENT_ID = Pattern.compile("[0-9a-f]{64}");
    static final Pattern SAMPLE_ID = Pattern.compile("[0-9a-f]{32}");

    private HistoryData() {
    }

    static long windowStart(long millis) {
        return Math.floorDiv(millis, WINDOW_MS) * WINDOW_MS;
    }

    static String iso(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }

    static Metric metric(JsonNode row) {
        String id = identifier(row.path("sample_id"), SAMPLE_ID);
        long time = time(row.path("sampled_at"));
        JsonNode disk = row.path("disks");
        JsonNode network = row.path("network");
        return new Metric(id, time, JsonValues.number(row.path("cpu_percent")),
                JsonValues.number(row.path("iowait_percent")),
                JsonValues.integer(row.path("memory").path("MemAvailable")),
                maxNumber(row.path("temperatures_c"), null),
                JsonValues.integer(disk.path("/overlay").path("free_bytes")),
                JsonValues.integer(disk.path("/docker").path("free_bytes")),
                JsonValues.integer(disk.path("/fourT").path("free_bytes")),
                maxNumber(row.path("disk_io"), "await_ms"), conntrackUsage(row.path("conntrack")),
                JsonValues.bool(row.path("wan").path("up")),
                linkUp(network.path("eth0")), linkUp(network.path("eth2")),
                row.path("errors").isObject() && !row.path("errors").isEmpty(), row.toString());
    }

    static Event event(JsonNode row) {
        String category = row.path("category").asText("system");
        String severity = row.path("severity").asText("unknown");
        String raw = row.path("raw").textValue();
        String type = EventClassifier.type(category, severity, raw);
        if (type == null) {
            type = "other";
        }
        String id = identifier(row.path("event_id"), EVENT_ID);
        long time = time(row.path("observed_at"));
        String source = row.path("source").textValue();
        String content = raw == null ? row.path("snapshot").toString() : raw;
        return new Event(id, time, shorten(category, 32), type, shorten(severity, 16),
                shorten(source, 512), shorten(content, 512), row.toString());
    }

    private static String identifier(JsonNode field, Pattern pattern) {
        String value = field.textValue();
        if (value == null || !pattern.matcher(value).matches()) {
            throw new IllegalArgumentException("Missing or invalid record ID");
        }
        return value;
    }

    private static long time(JsonNode field) {
        Instant value = JsonValues.instant(field);
        if (value == null) {
            throw new IllegalArgumentException("Missing or invalid record time");
        }
        return value.toEpochMilli();
    }

    static Double conntrackUsage(JsonNode conntrack) {
        Double count = JsonValues.number(conntrack.path("count"));
        Double max = JsonValues.number(conntrack.path("max"));
        return count != null && max != null && max > 0 ? 100 * count / max : null;
    }

    static Double maxNumber(JsonNode values, String field) {
        Double max = null;
        for (JsonNode value : values) {
            Double number = JsonValues.number(field == null ? value : value.path(field));
            if (number != null && (max == null || number > max)) {
                max = number;
            }
        }
        return max;
    }

    private static Boolean linkUp(JsonNode dev) {
        return switch (dev.path("operstate").asText()) {
            case "up" -> true;
            case "down", "lowerlayerdown" -> false;
            default -> null;
        };
    }

    private static String shorten(String text, int max) {
        if (text == null || text.length() <= max) {
            return text;
        }
        int end = Character.isHighSurrogate(text.charAt(max - 1)) ? max - 1 : max;
        return text.substring(0, end);
    }

    record Metric(String id, long time, Double cpu, Double ioWait, Long memoryAvailable,
                  Double temperature, Long overlayFree, Long dockerFree, Long archiveFree,
                  Double diskAwait, Double conntrackPercent, Boolean wanUp, Boolean eth0Up,
                  Boolean eth2Up, boolean hasErrors, String recordJson) {
    }

    record Event(String id, long time, String category, String type, String severity,
                 String source, String preview, String recordJson) {
    }

    record EventSummary(String id, String observedAt, String category, String type,
                        String severity, String source, String preview) {
    }

    record EventDetail(EventSummary event, String raw, JsonNode record) {
    }

    record EventPage(List<EventSummary> items, String nextCursor) {
    }

    record EventCursor(long time, String id) {
    }

    record Window(String start, int sampleCount, int errorSamples, Double cpuAveragePercent,
                  Double cpuMaxPercent, Double ioWaitMaxPercent, Long memoryAvailableMinBytes,
                  Double temperatureMaxC, Long overlayFreeMinBytes, Long dockerFreeMinBytes,
                  Long archiveFreeMinBytes, Double diskAwaitMaxMs, Double conntrackMaxPercent,
                  Integer wanDownSamples, Integer eth0DownSamples, Integer eth2DownSamples,
                  Double load1Max, Long swapUsedMaxBytes, Long swapTotalMaxBytes) {
        static Window empty(long millis) {
            return new Window(iso(millis), 0, 0, null, null, null, null, null, null,
                    null, null, null, null, null, null, null, null, null, null);
        }
    }
}
