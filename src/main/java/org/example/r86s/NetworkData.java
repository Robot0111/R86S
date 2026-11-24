package org.example.r86s;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

final class NetworkData {
    // The collector normally runs every 30 seconds. Longer outages remain gaps in the chart.
    static final long MAX_PAIR_GAP_MS = Duration.ofMinutes(2).toMillis();
    private static final Pattern NAME = Pattern.compile(
            "(?:[a-zA-Z0-9_.-]{1,64}|(?:xray|forwarded):[\\p{L}\\p{N}_.:@/+ -]{1,64})");

    private NetworkData() {
    }

    static boolean validName(String name) {
        return name != null && NAME.matcher(name).matches()
                && !(name.indexOf(':') >= 0 && name.substring(name.indexOf(':') + 1).isBlank());
    }

    static NetworkStatus status(StatusReader.Sampling sampling, JsonNode sample) {
        if (sampling.sampledAt() == null) {
            return new NetworkStatus(null, sampling.state(), sampling.ageSeconds(), null, List.of());
        }

        JsonNode network = sample.path("network");
        String internet = internetInterface(sample, network);
        var interfaces = new ArrayList<Interface>();
        for (var entry : network.properties()) {
            String name = entry.getKey();
            JsonNode link = entry.getValue();
            if (!validName(name) || !link.isObject()) {
                continue;
            }

            String kind = kind(name, link);
            boolean iptv = iptv(name, link);
            String scope = scope(name, kind, internet);
            boolean excludesIptv = !iptv && ("internet".equals(scope) || forwarded(name, link));
            Instant handshake = JsonValues.instant(link.path("last_handshake_at"));
            boolean eligible = !iptv && (excludesIptv || "xray".equals(scope) || "wireguard".equals(kind));
            interfaces.add(new Interface(name, kind, link.path("operstate").asText("unknown"),
                    JsonValues.bool(link.path("carrier")), nonnegative(link.path("speed_mbps")),
                    nonnegative(link.path("rx_bytes")), nonnegative(link.path("tx_bytes")), iptv,
                    link.path("device").textValue(), handshake == null ? null : handshake.toString(),
                    eligible, scope, !excludesIptv));
        }
        return new NetworkStatus(sampling.sampledAt(), sampling.state(), sampling.ageSeconds(), internet,
                List.copyOf(interfaces));
    }

    static boolean isIptvName(String name) {
        return name.toLowerCase(Locale.ROOT).contains("iptv");
    }

    private static boolean iptv(String name, JsonNode link) {
        return isIptvName(name)
                || Boolean.TRUE.equals(JsonValues.bool(link.path("iptv")))
                || "iptv".equalsIgnoreCase(link.path("kind").asText());
    }

    private static String internetInterface(JsonNode sample, JsonNode network) {
        String configured = sample.path("internet_interface").textValue();
        if (isLogicalWan(configured, network)) {
            return configured;
        }
        String wan = sample.path("wan").path("l3_device").textValue();
        return isLogicalWan(wan, network) ? wan : null;
    }

    private static boolean isLogicalWan(String name, JsonNode network) {
        if (!validName(name) || name.indexOf(':') >= 0) {
            return false;
        }
        JsonNode link = network.path(name);
        if (!link.isObject() || iptv(name, link)) {
            return false;
        }
        String kind = kind(name, link);
        return !isPhysical(name) && ("wan".equals(kind) || name.regionMatches(true, 0, "pppoe-", 0, 6));
    }

    private static String kind(String name, JsonNode link) {
        if (iptv(name, link)) {
            return "iptv";
        }
        if (isPhysical(name)) {
            return "ethernet";
        }
        return switch (link.path("kind").asText()) {
            case "wan", "wireguard", "bridge", "logical" -> link.path("kind").asText();
            default -> {
                if (name.startsWith("xray:")) {
                    yield "xray";
                }
                if (name.startsWith("forwarded:")) {
                    yield "forwarded";
                }
                if (name.matches("wg\\d+")) {
                    yield "wireguard";
                }
                yield name.startsWith("br-") ? "bridge" : "logical";
            }
        };
    }

    private static boolean isPhysical(String name) {
        return name.matches("eth\\d+");
    }

    private static boolean forwarded(String name, JsonNode link) {
        return name.startsWith("forwarded:") && "forwarded".equals(link.path("kind").asText())
                && Boolean.TRUE.equals(JsonValues.bool(link.path("iptv_excluded")));
    }

    static String scope(String interfaceName, String kind, String internetInterface) {
        if (interfaceName.equals(internetInterface)) {
            return "internet";
        }
        if ("xray".equals(kind) || interfaceName.startsWith("xray:")) {
            return "xray";
        }
        if ("forwarded".equals(kind) || interfaceName.startsWith("forwarded:")) {
            return "forwarded";
        }
        return "interface";
    }

    static Counter counter(String name, long time, JsonNode link, String bootId) {
        if (!link.isObject() || bootId == null || bootId.isBlank() || iptv(name, link)
                || (name.startsWith("forwarded:") && !forwarded(name, link))) {
            return null;
        }
        Long rx = nonnegative(link.path("rx_bytes"));
        Long tx = nonnegative(link.path("tx_bytes"));
        if (rx == null || tx == null) {
            return null;
        }
        return new Counter(time, bootId, link.path("counter_id").textValue(),
                nonnegative(link.path("ifindex")), rx, tx);
    }

    private static Long nonnegative(JsonNode node) {
        Long value = JsonValues.integer(node);
        return value != null && value >= 0 ? value : null;
    }

    record Counter(long time, String bootId, String counterId, Long ifindex, long rx, long tx) {
        boolean follows(Counter previous) {
            return previous != null && time > previous.time && time - previous.time <= MAX_PAIR_GAP_MS
                    && bootId.equals(previous.bootId) && Objects.equals(counterId, previous.counterId)
                    && Objects.equals(ifindex, previous.ifindex) && rx >= previous.rx && tx >= previous.tx;
        }
    }

    static final class TrafficAccumulator {
        private final long from;
        private final long to;
        private final Map<Long, Counts> counts = new HashMap<>();
        private Counter previous;

        TrafficAccumulator(long from, long to) {
            this.from = from;
            this.to = to;
        }

        void accept(Counter current) {
            if (current != null && current.follows(previous)) {
                // Attribute each measured delta to its ending sample's bucket.
                long start = HistoryData.windowStart(current.time);
                counts.computeIfAbsent(start, ignored -> new Counts()).add(
                        current.rx - previous.rx, current.tx - previous.tx);
            }
            previous = current;
        }

        List<TrafficWindow> windows() {
            List<TrafficWindow> windows = new ArrayList<>();
            for (long start = HistoryData.windowStart(from); start < to; start += HistoryData.WINDOW_MS) {
                Counts value = counts.get(start);
                windows.add(value == null || value.invalid
                        ? new TrafficWindow(HistoryData.iso(start), null, null, 0)
                        : new TrafficWindow(HistoryData.iso(start), value.rx, value.tx, value.samples));
            }
            return windows;
        }
    }

    private static final class Counts {
        long rx;
        long tx;
        int samples;
        boolean invalid;

        void add(long receive, long transmit) {
            // A corrupt or impossible cumulative counter must not overflow into a negative byte total.
            if (Long.MAX_VALUE - rx < receive || Long.MAX_VALUE - tx < transmit) {
                invalid = true;
                return;
            }
            rx += receive;
            tx += transmit;
            samples++;
        }
    }

    record Interface(String name, String kind, String state, Boolean carrier, Long speedMbps, Long rxBytes,
                     Long txBytes, boolean iptv, String device, String lastHandshakeAt, boolean trafficEligible,
                     String scope, boolean mayIncludeIptv) {
    }

    record NetworkStatus(String sampledAt, String state, Long ageSeconds, String internetInterface,
                         List<Interface> interfaces) {
        Interface find(String name) {
            return interfaces.stream().filter(link -> link.name.equals(name)).findFirst().orElse(null);
        }
    }

    record TrafficWindow(String start, Long rxBytes, Long txBytes, int sampleCount) {
    }
}
