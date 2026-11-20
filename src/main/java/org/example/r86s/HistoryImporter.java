package org.example.r86s;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.r86s.BatchArchive.Batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class HistoryImporter {
    private static final System.Logger LOG = System.getLogger(HistoryImporter.class.getName());
    private static final int MAX_BATCHES_PER_POLL = 12;
    private static final long MAINTENANCE_INTERVAL = Duration.ofHours(1).toNanos();

    private final AppConfig config;
    private final HistoryStore store;
    private final BatchArchive archives;
    private final DailyFileCleanup cleanup;
    private long lastMaintenance;
    private boolean maintenanceDone;

    HistoryImporter(AppConfig config, ObjectMapper json, HistoryStore store) {
        this.config = config;
        this.store = store;
        archives = new BatchArchive(json, config.hostId());
        cleanup = new DailyFileCleanup(config.monitorDir(), config.hostId(), json, store);
    }

    void run() {
        try {
            importOnce();
        } catch (SQLException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "History import will retry. Database error {0} ({1}).", e.getErrorCode(), e.getSQLState());
        } catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "History import will retry", e);
        }
    }

    void importOnce() throws IOException, SQLException {
        List<Path> files = sentBatches();
        long now = System.nanoTime();
        boolean maintenanceDue = !maintenanceDone || now - lastMaintenance >= MAINTENANCE_INTERVAL;
        if (files.isEmpty() && !maintenanceDue) {
            return;
        }

        Instant currentTime = Instant.now();
        long cutoff = currentTime.minus(Duration.ofDays(config.retentionDays())).toEpochMilli();
        try (Connection connection = store.connect()) {
            connection.setAutoCommit(false);
            importBatches(connection, files, cutoff);
            if (maintenanceDue && !Thread.currentThread().isInterrupted()) {
                if (config.pruneLocal()) {
                    cleanup.prune(connection, currentTime);
                }
                store.expireHistory(connection, cutoff);
                lastMaintenance = now;
                maintenanceDone = true;
            }
        }
    }

    private List<Path> sentBatches() throws IOException {
        Path directory = config.monitorDir().resolve("batches/sent");
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(file -> BatchArchive.batchId(file) != null)
                    .filter(file -> Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS))
                    .sorted().toList();
        } catch (NoSuchFileException e) {
            return List.of();
        }
    }

    private void importBatches(Connection connection, List<Path> files, long cutoff) throws SQLException {
        if (files.isEmpty()) {
            return;
        }
        Map<String, String> imported = store.importedBatches(connection, BatchArchive.batchId(files.getFirst()));
        int importedThisPoll = 0;
        for (Path file : files) {
            if (Thread.currentThread().isInterrupted() || importedThisPoll >= MAX_BATCHES_PER_POLL) {
                return;
            }
            String id = BatchArchive.batchId(file);
            String known = imported.get(id);
            if (known != null && !config.pruneLocal()) {
                continue;
            }
            try {
                Path ack = config.monitorDir().resolve("state").resolve(id + ".ack.json");
                String checksum = archives.awsChecksum(ack, id);
                if (known != null && !known.equals(checksum)) {
                    throw new IOException("Oracle and AWS confirmations have different checksums");
                }
                byte[] data = archives.verifiedBytes(file, checksum);
                if (known == null) {
                    Batch batch = archives.read(data, id, checksum);
                    if (batch.sealedMs() < cutoff) {
                        continue;
                    }
                    store.writeBatch(connection, batch);
                    importedThisPoll++;
                    LOG.log(System.Logger.Level.INFO, "Imported AWS batch {0}: {1} events, {2} samples",
                            id, batch.events().size(), batch.metrics().size());
                }
                if (config.pruneLocal()) {
                    Files.deleteIfExists(file);
                }
            } catch (IOException | IllegalArgumentException | ArithmeticException e) {
                LOG.log(System.Logger.Level.WARNING, "Keeping local archive " + id + ": " + e.getMessage());
            }
        }
    }
}
