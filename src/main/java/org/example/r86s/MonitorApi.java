package org.example.r86s;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import org.example.r86s.HistoryData.EventCursor;
import org.example.r86s.HistoryData.EventDetail;
import org.example.r86s.HistoryData.Window;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class MonitorApi {
    private static final System.Logger LOG = System.getLogger(MonitorApi.class.getName());
    private static final String VERSION = "/api/v1/";
    private static final String EVENTS = VERSION + "events";
    private static final long DAY_MS = Duration.ofDays(1).toMillis();
    private static final long MAX_EPOCH_MS = 253_402_300_799_999L;

    private final ObjectMapper json;
    private final StatusReader status;
    private final HistoryStore store;

    MonitorApi(ObjectMapper json, StatusReader status, HistoryStore store) {
        this.json = json;
        this.status = status;
        this.store = store;
    }

    void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getRawPath();
            boolean detail = path.startsWith(EVENTS + "/")
                    && HistoryData.EVENT_ID.matcher(path.substring(EVENTS.length() + 1)).matches();
            if (!detail && !Set.of(VERSION + "status", VERSION + "metrics", EVENTS).contains(path)) {
                error(exchange, 404, "not_found");
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                error(exchange, 405, "method_not_allowed");
                return;
            }

            try {
                if (path.equals(VERSION + "status")) {
                    respond(exchange, 200, status.read());
                } else if (store == null) {
                    error(exchange, 503, "history_disabled");
                } else if (detail) {
                    EventDetail event = store.event(path.substring(EVENTS.length() + 1));
                    if (event == null) {
                        error(exchange, 404, "event_not_found");
                    } else {
                        respond(exchange, 200, event);
                    }
                } else if (path.equals(EVENTS)) {
                    events(exchange, query(exchange, Set.of("from", "to", "type", "all", "limit", "cursor")));
                } else {
                    metrics(exchange, query(exchange, Set.of("from", "to")));
                }
            } catch (IllegalArgumentException e) {
                respond(exchange, 400, Map.of("error", "invalid_query", "message", e.getMessage()));
            } catch (SQLException e) {
                LOG.log(System.Logger.Level.WARNING, "History query failed. Database error {0} ({1}).",
                        e.getErrorCode(), e.getSQLState());
                error(exchange, 503, "history_unavailable");
            }
        }
    }

    private void metrics(HttpExchange exchange, Map<String, String> query) throws IOException, SQLException {
        Range range = range(query);
        if (range.to() - range.from() > 31 * DAY_MS) {
            throw new IllegalArgumentException("Metrics can be queried for up to 31 days per request");
        }
        respond(exchange, 200, new MetricsResponse(HistoryData.iso(range.from()), HistoryData.iso(range.to()),
                300, store.windows(range.from(), range.to())));
    }

    private void events(HttpExchange exchange, Map<String, String> query) throws IOException, SQLException {
        Range range = range(query);
        String type = query.get("type");
        if (type != null && !type.matches("[a-z0-9_]{1,32}")) {
            throw new IllegalArgumentException("Invalid event type");
        }
        String all = query.getOrDefault("all", "false");
        if (!"true".equals(all) && !"false".equals(all)) {
            throw new IllegalArgumentException("all must be true or false");
        }
        int limit;
        try {
            limit = Integer.parseInt(query.getOrDefault("limit", "50"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("limit must be an integer", e);
        }
        if (limit < 1 || limit > 200) {
            throw new IllegalArgumentException("limit must be between 1 and 200");
        }
        EventCursor cursor = decodeCursor(query.get("cursor"));
        var result = store.events(range.from(), range.to(), type, cursor, limit, Boolean.parseBoolean(all));
        String next = result.next() == null ? null : Base64.getUrlEncoder().withoutPadding().encodeToString(
                (result.next().time() + ":" + result.next().id()).getBytes(StandardCharsets.US_ASCII));
        respond(exchange, 200, new HistoryData.EventPage(result.items(), next));
    }

    private static EventCursor decodeCursor(String encoded) {
        if (encoded == null) {
            return null;
        }
        if (encoded.length() > 128) {
            throw new IllegalArgumentException("Invalid cursor");
        }
        try {
            String[] fields = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.US_ASCII)
                    .split(":", 2);
            if (fields.length == 2 && HistoryData.EVENT_ID.matcher(fields[1]).matches()) {
                return new EventCursor(Long.parseLong(fields[0]), fields[1]);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid cursor", e);
        }
        throw new IllegalArgumentException("Invalid cursor");
    }

    private static Range range(Map<String, String> query) {
        long to = date(query.get("to"), System.currentTimeMillis());
        long from = date(query.get("from"), Math.max(0, to - DAY_MS));
        if (from < 0 || to > MAX_EPOCH_MS || from >= to) {
            throw new IllegalArgumentException("Expected an increasing from/to range");
        }
        return new Range(from, to);
    }

    private static long date(String value, long fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Instant.parse(value).toEpochMilli();
        } catch (DateTimeParseException | ArithmeticException e) {
            throw new IllegalArgumentException("Use ISO 8601 timestamps with a time zone", e);
        }
    }

    private static Map<String, String> query(HttpExchange exchange, Set<String> allowed) {
        Map<String, String> query = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return query;
        }
        if (raw.length() > 2048) {
            throw new IllegalArgumentException("Query is too long");
        }
        for (String part : raw.split("&", -1)) {
            String[] pair = part.split("=", 2);
            String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);
            if (pair.length != 2 || !allowed.contains(key) || query.containsKey(key)) {
                throw new IllegalArgumentException("Unknown, missing or duplicated query parameter");
            }
            query.put(key, URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
        }
        return query;
    }

    private void error(HttpExchange exchange, int code, String message) throws IOException {
        respond(exchange, code, Map.of("error", message));
    }

    private void respond(HttpExchange exchange, int code, Object payload) throws IOException {
        byte[] body = json.writeValueAsBytes(payload);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(code, body.length);
        exchange.getResponseBody().write(body);
    }

    private record Range(long from, long to) {
    }

    private record MetricsResponse(String from, String to, int windowSeconds, List<Window> windows) {
    }
}
