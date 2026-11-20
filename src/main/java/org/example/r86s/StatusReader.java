package org.example.r86s;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

final class StatusReader {
    private static final Duration STALE_AFTER = Duration.ofSeconds(90);

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
        Wan wan = sampling.sampledAt() == null ? null : wan(sample.data().path("wan"));
        return new Status(sampling, wan, upload(upload));
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

    record Status(Sampling sampling, Wan wan, AwsUpload awsUpload) {
    }

    record Sampling(String state, String sampledAt, Long ageSeconds) {
    }

    record Wan(Boolean up, Boolean pending, Long uptimeSeconds, String l3Device, String device) {
    }

    record AwsUpload(String state, String confirmedAt, String batchId) {
    }
}
