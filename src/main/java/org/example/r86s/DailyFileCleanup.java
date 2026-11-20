package org.example.r86s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

final class DailyFileCleanup {
    private static final System.Logger LOG = System.getLogger(DailyFileCleanup.class.getName());
    private static final int IDS_PER_QUERY = 500;

    private final Path root;
    private final String host;
    private final ObjectMapper json;
    private final HistoryStore store;

    DailyFileCleanup(Path root, String host, ObjectMapper json, HistoryStore store) {
        this.root = root;
        this.host = host;
        this.json = json;
        this.store = store;
    }

    void prune(Connection connection, Instant now) throws SQLException {
        LocalDate today = now.atOffset(ZoneOffset.UTC).toLocalDate();
        Instant lastWriteCutoff = now.minus(Duration.ofDays(1));
        pruneDirectory(connection, root.resolve("raw"), false, today, lastWriteCutoff);
        pruneDirectory(connection, root.resolve("metrics"), true, today, lastWriteCutoff);
    }

    private void pruneDirectory(Connection connection, Path directory, boolean metrics, LocalDate today, Instant cutoff)
            throws SQLException {
        try (Stream<Path> days = Files.list(directory)) {
            for (Path day : days.filter(path -> closedDay(path, today)).toList()) {
                try (Stream<Path> files = Files.list(day)) {
                    for (Path file : files.filter(path -> candidate(path, metrics)).toList()) {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        pruneFile(connection, file, metrics, cutoff);
                    }
                } catch (NoSuchFileException ignored) {
                    // The collector's retention pass removed the directory first.
                }
            }
        } catch (NoSuchFileException ignored) {
            // This installation has no files for this source yet.
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "Cannot inspect closed history directories", e);
        }
    }

    private static boolean closedDay(Path directory, LocalDate today) {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        try {
            return LocalDate.parse(directory.getFileName().toString()).isBefore(today);
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private static boolean candidate(Path file, boolean metrics) {
        String name = file.getFileName().toString();
        return (metrics ? "samples.jsonl".equals(name) : name.endsWith(".jsonl"))
                && Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS);
    }

    private void pruneFile(Connection connection, Path file, boolean metrics, Instant cutoff) throws SQLException {
        try {
            var before = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!before.isRegularFile() || before.lastModifiedTime().toInstant().isAfter(cutoff)
                    || !completeLastLine(file, before.size()) || !allRecorded(connection, file, metrics)) {
                return;
            }
            var after = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (after.isRegularFile() && Objects.equals(before.fileKey(), after.fileKey())
                    && before.size() == after.size() && before.lastModifiedTime().equals(after.lastModifiedTime())) {
                Files.deleteIfExists(file);
            }
        } catch (NoSuchFileException ignored) {
            // A separate retention pass removed the file.
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "Keeping local file " + root.relativize(file), e);
        }
    }

    private static boolean completeLastLine(Path file, long size) throws IOException {
        if (size == 0) {
            return true;
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            var lastByte = ByteBuffer.allocate(1);
            return channel.read(lastByte, size - 1) == 1 && lastByte.get(0) == '\n';
        }
    }

    private boolean allRecorded(Connection connection, Path file, boolean metrics) throws IOException, SQLException {
        String key = metrics ? "sample_id" : "event_id";
        Pattern format = metrics ? HistoryData.SAMPLE_ID : HistoryData.EVENT_ID;
        Set<String> ids = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode row = json.readTree(line);
                if (row == null || !host.equals(row.path("host_id").asText())) {
                    return false;
                }
                String id = row.path(key).asText();
                if (!format.matcher(id).matches()) {
                    return false;
                }
                ids.add(id);
                if (ids.size() == IDS_PER_QUERY) {
                    if (!store.containsAll(connection, metrics, ids)) {
                        return false;
                    }
                    ids.clear();
                }
            }
        }
        return ids.isEmpty() || store.containsAll(connection, metrics, ids);
    }
}
