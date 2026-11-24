package org.example.r86s;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

final class StatusReader {
    private static final Duration STALE_AFTER = Duration.ofSeconds(90);
    private static final Duration BANDWIDTH_STALE_AFTER = Duration.ofSeconds(20);
    private static final Pattern CLIENT_TRAFFIC_TOKEN = Pattern.compile("[A-Za-z0-9_-]{32,128}");

    private final Path stateDir;
    private final ObjectMapper json;

    StatusReader(Path stateDir, ObjectMapper json) {
        this.stateDir = stateDir;
        this.json = json;
    }

    Status read() {
        Snapshot sample = readFile("latest-sample.json");
        Snapshot upload = readFile("latest-upload.json");
        Sampling sampling = sampling(sample);
        if (sampling.sampledAt() == null) {
            return new Status(sampling, null, upload(upload), null);
        }
        return new Status(sampling, wan(sample.data().path("wan")), upload(upload),
                liveMetrics(sample.data(), sampling.sampledAt()));
    }

    NetworkData.NetworkStatus network() {
        Snapshot sample = readFile("latest-sample.json");
        return NetworkData.status(sampling(sample), sample.data());
    }

    JsonNode bandwidth(String name) {
        Snapshot snapshot = readFile("latest-bandwidth.json");
        if (snapshot.error() != null) {
            return null;
        }
        JsonNode data = snapshot.data();
        Instant sampledAt = JsonValues.instant(data.path("sampledAt"));
        if (sampledAt == null) {
            return null;
        }
        Duration age = Duration.between(sampledAt, Instant.now());
        if (age.isNegative() || age.compareTo(BANDWIDTH_STALE_AFTER) > 0) {
            return null;
        }
        JsonNode series = data.path("interfaces").path(name);
        JsonNode windows = series.path("windows");
        return name.equals(series.path("interfaceName").textValue())
                && "luci".equals(series.path("scope").textValue())
                && Boolean.FALSE.equals(JsonValues.bool(series.path("mayIncludeIptv")))
                && series.path("windowSeconds").asInt(0) == 3
                && windows.isArray() && !windows.isEmpty() && windows.size() <= 600 ? series : null;
    }

    boolean clientTrafficAuthorized(String authorization) {
        if (authorization == null || authorization.length() > 135
                || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return false;
        }
        String provided = authorization.substring(7);
        if (!CLIENT_TRAFFIC_TOKEN.matcher(provided).matches()) {
            return false;
        }
        try (InputStream input = Files.newInputStream(stateDir.resolve("client-traffic.token"))) {
            byte[] bytes = input.readNBytes(131);
            if (bytes.length == 131) {
                return false;
            }
            String expected = new String(bytes, StandardCharsets.US_ASCII).strip();
            return CLIENT_TRAFFIC_TOKEN.matcher(expected).matches()
                    && MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII),
                    provided.getBytes(StandardCharsets.US_ASCII));
        } catch (IOException e) {
            return false;
        }
    }

    private Snapshot readFile(String name) {
        try (InputStream in = Files.newInputStream(stateDir.resolve(name))) {
            JsonNode data = json.readTree(in);
            if (data == null || !data.isObject()) {
                return new Snapshot(null, "invalid");
            }
            return new Snapshot(data, null);
        } catch (NoSuchFileException e) {
            return new Snapshot(null, "missing");
        } catch (JsonProcessingException e) {
            return new Snapshot(null, "invalid");
        } catch (IOException e) {
            return new Snapshot(null, "unreadable");
        }
    }

    private static Sampling sampling(Snapshot sample) {
        if (sample.error() != null) {
            return new Sampling(sample.error(), null, null);
        }
        Instant sampledAt = JsonValues.instant(sample.data().path("sampled_at"));
        if (sampledAt == null) {
            return new Sampling("invalid", null, null);
        }
        Duration age = Duration.between(sampledAt, Instant.now());
        String state;
        if (age.isNegative()) {
            state = "clock_skew";
        } else if (age.compareTo(STALE_AFTER) > 0) {
            state = "stale";
        } else {
            state = "fresh";
        }
        return new Sampling(state, sampledAt.toString(), age.getSeconds());
    }

    private static Wan wan(JsonNode node) {
        if (!node.isObject()) {
            return null;
        }
        return new Wan(
                JsonValues.bool(node.path("up")),
                JsonValues.bool(node.path("pending")),
                JsonValues.integer(node.path("uptime")),
                node.path("l3_device").textValue(),
                node.path("device").textValue());
    }

    private static LiveMetrics liveMetrics(JsonNode sample, String sampledAt) {
        JsonNode load = sample.path("load");
        JsonNode memory = sample.path("memory");
        Long swapTotal = nonnegative(memory.path("SwapTotal"));
        Long swapFree = nonnegative(memory.path("SwapFree"));
        Long swapUsed = swapTotal != null && swapFree != null && swapFree <= swapTotal ? swapTotal - swapFree : null;
        return new LiveMetrics(sampledAt, JsonValues.number(sample.path("cpu_percent")),
                JsonValues.number(load.path(0)), JsonValues.number(load.path(1)), JsonValues.number(load.path(2)),
                JsonValues.number(sample.path("iowait_percent")), nonnegative(memory.path("MemAvailable")),
                swapUsed, swapTotal, HistoryData.maxNumber(sample.path("temperatures_c"), null),
                HistoryData.maxNumber(sample.path("disk_io"), "await_ms"),
                HistoryData.conntrackUsage(sample.path("conntrack")), filesystems(sample.path("disks")),
                directories(sample.path("directories")));
    }

    private static List<FileSystemSpace> filesystems(JsonNode disks) {
        var results = new ArrayList<FileSystemSpace>();
        for (var entry : disks.properties()) {
            JsonNode disk = entry.getValue();
            Long total = nonnegative(disk.path("total_bytes"));
            Long available = nonnegative(disk.path("free_bytes"));
            if (total == null || total == 0 || available == null || available > total) {
                continue;
            }
            Long used = nonnegative(disk.path("used_bytes"));
            results.add(new FileSystemSpace(entry.getKey(), disk.path("device").textValue(),
                    disk.path("filesystem_id").textValue(), total, used != null && used <= total ? used : null, available,
                    disk.path("mount_root").textValue()));
        }
        return results;
    }

    private static List<DirectorySpace> directories(JsonNode values) {
        var results = new ArrayList<DirectorySpace>();
        if (values.isArray()) {
            for (JsonNode value : values) {
                String path = value.path("path").textValue();
                Long used = nonnegative(value.path("used_bytes"));
                Instant measuredAt = JsonValues.instant(value.path("measured_at"));
                if (path != null && path.startsWith("/") && used != null && measuredAt != null) {
                    results.add(new DirectorySpace(path, used, measuredAt.toString()));
                }
            }
        }
        return results;
    }

    private static Long nonnegative(JsonNode value) {
        Long number = JsonValues.integer(value);
        return number != null && number >= 0 ? number : null;
    }

    private static AwsUpload upload(Snapshot file) {
        if (file.error() != null) {
            return new AwsUpload(file.error(), null, null);
        }
        Instant confirmedAt = JsonValues.instant(file.data().path("confirmed_at"));
        String batchId = file.data().path("batch_id").textValue();
        if (confirmedAt == null || batchId == null || batchId.isBlank()) {
            return new AwsUpload("invalid", null, null);
        }
        return new AwsUpload("confirmed", confirmedAt.toString(), batchId);
    }

    private record Snapshot(JsonNode data, String error) {
    }

    record Status(Sampling sampling, Wan wan, AwsUpload awsUpload, LiveMetrics liveMetrics) {
    }

    record Sampling(String state, String sampledAt, Long ageSeconds) {
    }

    record Wan(Boolean up, Boolean pending, Long uptimeSeconds, String l3Device, String device) {
    }

    record AwsUpload(String state, String confirmedAt, String batchId) {
    }

    record LiveMetrics(String sampledAt, Double cpuPercent, Double load1, Double load5, Double load15,
                       Double ioWaitPercent, Long memoryAvailableBytes, Long swapUsedBytes, Long swapTotalBytes,
                       Double temperatureC, Double diskAwaitMs, Double conntrackPercent,
                       List<FileSystemSpace> filesystems, List<DirectorySpace> directories) {
    }

    record FileSystemSpace(String mount, String device, String filesystemId, Long totalBytes, Long usedBytes,
                           Long availableBytes, String mountRoot) {
    }

    record DirectorySpace(String path, Long usedBytes, String measuredAt) {
    }
}
