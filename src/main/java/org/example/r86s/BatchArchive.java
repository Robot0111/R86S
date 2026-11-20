package org.example.r86s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.example.r86s.HistoryData.Event;
import org.example.r86s.HistoryData.Metric;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

final class BatchArchive {
    static final String SUFFIX = ".tar.gz";
    private static final Pattern BATCH_ID = Pattern.compile("r86s-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}");
    private static final Pattern CHECKSUM = Pattern.compile("[0-9a-f]{64}");
    private static final String EVENTS = "events.jsonl";
    private static final String METRICS = "metrics.jsonl";
    private static final Set<String> PAYLOAD = Set.of(EVENTS, METRICS, "summary.json");
    private static final String MANIFEST = "manifest.json";
    private static final int MAX_MEMBER = 32 * 1024 * 1024;
    private static final int MAX_BATCH = 64 * 1024 * 1024;

    private final ObjectMapper json;
    private final String host;

    BatchArchive(ObjectMapper json, String host) {
        this.json = json;
        this.host = host;
    }

    static String batchId(Path file) {
        String name = file.getFileName().toString();
        if (!name.endsWith(SUFFIX)) {
            return null;
        }
        String id = name.substring(0, name.length() - SUFFIX.length());
        return BATCH_ID.matcher(id).matches() ? id : null;
    }

    String awsChecksum(Path ack, String id) throws IOException {
        byte[] bytes;
        try (InputStream in = Files.newInputStream(ack, LinkOption.NOFOLLOW_LINKS)) {
            bytes = in.readNBytes(4097);
        }
        if (bytes.length > 4096) {
            throw new IOException("AWS acknowledgement exceeds size limit");
        }
        JsonNode data = object(bytes);
        String digest = data.path("sha256").asText();
        String status = data.path("status").asText();
        if (!id.equals(data.path("batch_id").asText()) || !CHECKSUM.matcher(digest).matches()
                || !("accepted".equals(status) || "duplicate".equals(status))) {
            throw new IOException("AWS acknowledgement does not match batch");
        }
        return digest;
    }

    byte[] verifiedBytes(Path file, String checksum) throws IOException {
        byte[] bytes;
        try (InputStream in = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            bytes = in.readNBytes(MAX_BATCH + 1);
        }
        if (bytes.length == 0 || bytes.length > MAX_BATCH || !checksum.equals(sha256(bytes))) {
            throw new IOException("Archive size or SHA-256 does not match the AWS acknowledgement");
        }
        return bytes;
    }

    Batch read(byte[] bytes, String id, String checksum) throws IOException {
        Map<String, byte[]> members = new HashMap<>();
        long total = 0;
        try (var tar = new TarArchiveInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))) {
            TarArchiveEntry entry;
            while ((entry = tar.getNextEntry()) != null) {
                String name = entry.getName();
                long size = entry.getSize();
                total += size;
                if (!(PAYLOAD.contains(name) || MANIFEST.equals(name)) || members.containsKey(name)
                        || !entry.isFile() || entry.isSparse() || !tar.canReadEntryData(entry)
                        || size < 0 || size > MAX_MEMBER || total > MAX_BATCH) {
                    throw new IOException("Unexpected or oversized archive member");
                }
                byte[] data = tar.readNBytes((int) size);
                if (data.length != size) {
                    throw new IOException("Truncated archive member");
                }
                members.put(name, data);
            }
        }
        if (members.size() != PAYLOAD.size() + 1 || !members.containsKey(MANIFEST)) {
            throw new IOException("Missing archive member");
        }
        JsonNode manifest = object(members.get(MANIFEST));
        JsonNode summary = object(members.get("summary.json"));
        if (manifest.path("format_version").asInt() != 1 || !id.equals(manifest.path("batch_id").asText())
                || !id.equals(summary.path("batch_id").asText()) || !host.equals(manifest.path("host_id").asText())) {
            throw new IOException("Archive manifest or summary identity mismatch");
        }
        JsonNode checksums = manifest.path("files");
        if (!checksums.isObject() || checksums.size() != PAYLOAD.size()) {
            throw new IOException("Invalid manifest file list");
        }
        for (String name : PAYLOAD) {
            JsonNode expected = checksums.path(name);
            byte[] data = members.get(name);
            if (data == null || expected.path("bytes").asLong(-1) != data.length
                    || !sha256(data).equals(expected.path("sha256").asText())) {
                throw new IOException("Manifest checksum mismatch for " + name);
            }
        }

        Instant sealedAt = JsonValues.instant(manifest.path("sealed_at"));
        if (sealedAt == null) {
            throw new IOException("Missing sealed_at");
        }
        List<Metric> metrics = records(members.get(METRICS), HistoryData::metric);
        List<Event> events = records(members.get(EVENTS), HistoryData::event);
        JsonNode counts = manifest.path("counts");
        if (metrics.size() != counts.path(METRICS).asInt(0) || events.size() != counts.path(EVENTS).asInt(0)) {
            throw new IOException("Archive record count mismatch");
        }
        return new Batch(id, checksum, sealedAt.toEpochMilli(), metrics, events);
    }

    private <T> List<T> records(byte[] bytes, java.util.function.Function<JsonNode, T> parser) throws IOException {
        if (bytes.length > 0 && bytes[bytes.length - 1] != '\n') {
            throw new IOException("JSONL archive member has a partial final line");
        }
        List<T> rows = new ArrayList<>();
        try (var reader = new BufferedReader(new InputStreamReader(
                new ByteArrayInputStream(bytes), StandardCharsets.UTF_8.newDecoder()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode node = json.readTree(line);
                if (node == null || !node.isObject() || !host.equals(node.path("host_id").asText())) {
                    throw new IOException("Invalid JSONL record or host");
                }
                try {
                    rows.add(parser.apply(node));
                } catch (IllegalArgumentException | ArithmeticException e) {
                    throw new IOException("Invalid archived record", e);
                }
            }
        }
        return rows;
    }

    private JsonNode object(byte[] bytes) throws IOException {
        JsonNode data = json.readTree(bytes);
        if (data == null || !data.isObject()) {
            throw new IOException("Expected a JSON object");
        }
        return data;
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    record Batch(String id, String checksum, long sealedMs, List<Metric> metrics, List<Event> events) {
    }
}
