package org.example.r86s;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

record AppConfig(Path monitorDir, String bindAddress, int port, String hostId,
                 int importSeconds, int retentionDays, boolean pruneLocal, Database database) {
    static AppConfig load(Map<String, String> env) {
        Path monitorDir = Path.of(env.getOrDefault("R86S_MONITOR_DIR", "/monitor"));
        String bind = env.getOrDefault("R86S_HTTP_BIND", "0.0.0.0");
        int port = integer(env, "R86S_HTTP_PORT", 8080, 0, 65535);
        int importSeconds = integer(env, "R86S_IMPORT_SECONDS", 30, 1, 3600);
        int retentionDays = integer(env, "R86S_DB_RETENTION_DAYS", 180, 1, 3650);
        boolean pruneLocal = Boolean.parseBoolean(env.getOrDefault("R86S_PRUNE_LOCAL", "false"));
        String hostId = env.getOrDefault("R86S_HOST_ID", "r86s");
        if (hostId.isBlank() || hostId.length() > 128) {
            throw new IllegalArgumentException("R86S_HOST_ID must contain 1 to 128 characters");
        }

        String url = env.getOrDefault("R86S_DB_URL", "");
        String user = env.getOrDefault("R86S_DB_USER", "");
        String password = env.getOrDefault("R86S_DB_PASSWORD", "");
        Database database = null;
        if (!url.isBlank() || !user.isBlank() || !password.isEmpty()) {
            if (url.isBlank() || user.isBlank() || password.isEmpty()) {
                throw new IllegalArgumentException("Set R86S_DB_URL, R86S_DB_USER and R86S_DB_PASSWORD together");
            }
            database = new Database(url.trim(), user.trim(), password);
        }
        return new AppConfig(monitorDir, bind, port, hostId, importSeconds, retentionDays, pruneLocal, database);
    }

    private static int integer(Map<String, String> env, String name, int fallback, int min, int max) {
        int value;
        try {
            value = Integer.parseInt(env.getOrDefault(name, Integer.toString(fallback)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
        if (value < min || value > max) {
            throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
        }
        return value;
    }

    static final class Database {
        private final String url;
        private final Properties properties = new Properties();

        Database(String url, String user, String password) {
            this.url = url;
            properties.setProperty("user", user);
            properties.setProperty("password", password);
            properties.setProperty("oracle.net.CONNECT_TIMEOUT", "5000");
            properties.setProperty("oracle.jdbc.ReadTimeout", "15000");
        }

        Connection connect() throws SQLException {
            return DriverManager.getConnection(url, properties);
        }
    }
}
