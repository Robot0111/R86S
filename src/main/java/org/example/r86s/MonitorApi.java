package org.example.r86s;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import org.example.r86s.HistoryData.EventCursor;
import org.example.r86s.HistoryData.EventDetail;
import org.example.r86s.HistoryData.Window;
import org.example.r86s.NetworkData.TrafficWindow;

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
    private static final String NETWORK = VERSION + "network";
    private static final String TRAFFIC = NETWORK + "/traffic";
    private static final String BANDWIDTH = NETWORK + "/bandwidth";
    private static final String CLIENT_TRAFFIC = VERSION + "client-traffic";
    private static final String AWS_USAGE = CLIENT_TRAFFIC + "/aws-usage";
    private static final String TRAFFIC_RULES = VERSION + "traffic-rules";
    private static final int MAX_RULE_BODY = 4_096;
    private static final long DAY_MS = Duration.ofDays(1).toMillis();
    private static final long MAX_CLIENT_TRAFFIC_MS = 60 * DAY_MS;
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
            if (path.equals(TRAFFIC_RULES) || path.startsWith(TRAFFIC_RULES + "/")) {
                trafficRules(exchange, path);
                return;
            }
            boolean detail = path.startsWith(EVENTS + "/")
                    && HistoryData.EVENT_ID.matcher(path.substring(EVENTS.length() + 1)).matches();
            if (!detail && !Set.of(VERSION + "status", VERSION + "metrics", EVENTS, NETWORK, TRAFFIC, BANDWIDTH,
                    CLIENT_TRAFFIC, AWS_USAGE).contains(path)) {
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
                } else if (path.equals(NETWORK)) {
                    respond(exchange, 200, status.network());
                } else if (path.equals(BANDWIDTH)) {
                    bandwidth(exchange, query(exchange, Set.of("interface")));
                } else if ((path.equals(CLIENT_TRAFFIC) || path.equals(AWS_USAGE))
                        && !authorizeTraffic(exchange)) {
                    return;
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
                } else if (path.equals(TRAFFIC)) {
                    traffic(exchange, query(exchange, Set.of("from", "to", "interface")));
                } else if (path.equals(CLIENT_TRAFFIC)) {
                    clientTraffic(exchange, query(exchange, Set.of("from", "to", "scope", "region", "route", "client", "category")));
                } else if (path.equals(AWS_USAGE)) {
                    Range range = clientTrafficRange(query(exchange, Set.of("from", "to")));
                    respond(exchange, 200, store.awsUsage(range.from(), range.to()));
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

    private boolean authorizeTraffic(HttpExchange exchange) throws IOException {
        if (status.clientTrafficAuthorized(exchange.getRequestHeaders().getFirst("Authorization"))) return true;
        exchange.getResponseHeaders().set("WWW-Authenticate", "Bearer");
        error(exchange, 401, "client_traffic_unauthorized");
        return false;
    }

    private void trafficRules(HttpExchange exchange, String path) throws IOException {
        if (!authorizeTraffic(exchange)) return;
        if (store == null) {
            error(exchange, 503, "history_disabled");
            return;
        }
        boolean collection = path.equals(TRAFFIC_RULES);
        String id = collection ? null : path.substring(TRAFFIC_RULES.length() + 1);
        if (id != null && !TrafficRule.validId(id)) {
            error(exchange, 404, "rule_not_found");
            return;
        }
        String method = exchange.getRequestMethod();
        if (!(collection ? Set.of("GET", "POST") : Set.of("PUT", "DELETE")).contains(method)) {
            exchange.getResponseHeaders().set("Allow", collection ? "GET, POST" : "PUT, DELETE");
            error(exchange, 405, "method_not_allowed");
            return;
        }

        var rules = store.trafficRules();
        TrafficRule submitted = null;
        try {
            query(exchange, Set.of());
            switch (method) {
                case "GET" -> respond(exchange, 200, new TrafficRule.Page(rules.list()));
                case "POST" -> {
                    submitted = readRule(exchange, null);
                    rules.insert(submitted);
                    respond(exchange, 201, submitted);
                }
                case "PUT" -> {
                    submitted = readRule(exchange, id);
                    if (rules.update(submitted)) respond(exchange, 200, submitted);
                    else error(exchange, 404, "rule_not_found");
                }
                case "DELETE" -> {
                    if (rules.delete(id)) {
                        exchange.getResponseHeaders().set("Cache-Control", "no-store");
                        exchange.sendResponseHeaders(204, -1);
                    } else {
                        error(exchange, 404, "rule_not_found");
                    }
                }
                default -> throw new IllegalStateException("Unexpected traffic rules method");
            }
        } catch (IllegalArgumentException e) {
            respond(exchange, 400, Map.of("error", "invalid_rule", "message", e.getMessage()));
        } catch (SQLException e) {
            if (TrafficRuleStore.missingTable(e)) {
                error(exchange, 503, "rules_schema_missing");
            } else if (submitted != null && "client_ip".equals(submitted.kind())
                    && TrafficRuleStore.legacyKindConstraint(e)) {
                error(exchange, 503, "rules_client_ip_migration_required");
            } else if (TrafficRuleStore.duplicate(e)) {
                error(exchange, 409, "rule_conflict");
            } else {
                LOG.log(System.Logger.Level.WARNING, "Traffic rules operation failed. Database error {0} ({1}).",
                        e.getErrorCode(), e.getSQLState());
                error(exchange, 503, "rules_unavailable");
            }
        }
    }

    private TrafficRule readRule(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readNBytes(MAX_RULE_BODY + 1);
        if (body.length > MAX_RULE_BODY) throw new IllegalArgumentException("规则请求过大");
        JsonNode input;
        try {
            input = json.reader().with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).readTree(body);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("请输入有效 JSON", e);
        }
        if (input == null || !input.isObject() || input.size() != 3 || !input.path("kind").isTextual()
                || !input.path("target").isTextual() || !input.path("label").isTextual()) {
            throw new IllegalArgumentException("规则须包含 kind、target、label 三个文本字段");
        }
        return TrafficRule.create(id, input.path("kind").textValue(), input.path("target").textValue(),
                input.path("label").textValue());
    }

    private void bandwidth(HttpExchange exchange, Map<String, String> query) throws IOException {
        String name = requiredInterface(query);
        if (NetworkData.isIptvName(name)) {
            throw new IllegalArgumentException("IPTV interfaces are excluded from bandwidth");
        }
        var current = status.network();
        var link = current.find(name);
        if (link != null && link.iptv()) {
            throw new IllegalArgumentException("IPTV interfaces are excluded from bandwidth");
        }
        if (link != null && (name.equals(current.internetInterface()) || "wireguard".equals(link.kind()))) {
            var series = status.bandwidth(name);
            if (series != null) {
                respond(exchange, 200, series);
                return;
            }
        }
        error(exchange, 503, "bandwidth_unavailable");
    }

    private void traffic(HttpExchange exchange, Map<String, String> query) throws IOException, SQLException {
        Range range = range(query);
        if (range.to() - range.from() > 7 * DAY_MS) {
            throw new IllegalArgumentException("Traffic can be queried for up to 7 days per request");
        }
        String name = requiredInterface(query);
        var current = status.network();
        var link = current.find(name);
        if (NetworkData.isIptvName(name) || link != null && link.iptv()) {
            throw new IllegalArgumentException("IPTV interfaces are excluded from traffic history");
        }
        String scope = NetworkData.scope(name, link == null ? null : link.kind(), current.internetInterface());
        boolean mayIncludeIptv = !"internet".equals(scope) && !"forwarded".equals(scope);
        respond(exchange, 200, new TrafficResponse(name, scope, mayIncludeIptv,
                HistoryData.iso(range.from()), HistoryData.iso(range.to()), 300,
                store.traffic(range.from(), range.to(), name)));
    }

    private static String requiredInterface(Map<String, String> query) {
        String name = query.get("interface");
        if (!NetworkData.validName(name)) {
            throw new IllegalArgumentException("A valid interface name is required");
        }
        return name;
    }

    private void clientTraffic(HttpExchange exchange, Map<String, String> query) throws IOException, SQLException {
        Range range = clientTrafficRange(query);
        var filters = ClientTraffic.Filters.from(query);
        respond(exchange, 200, store.clientTraffic(range.from(), range.to(), filters));
    }

    private static Range clientTrafficRange(Map<String, String> query) {
        Range range = range(query);
        if (range.to() - range.from() > MAX_CLIENT_TRAFFIC_MS) {
            throw new IllegalArgumentException("Client traffic can be queried for up to 60 days per request");
        }
        return range;
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

    private record TrafficResponse(@JsonProperty("interface") String interfaceName, String scope, boolean mayIncludeIptv,
                                   String from, String to,
                                   int windowSeconds, List<TrafficWindow> windows) {
    }
}
