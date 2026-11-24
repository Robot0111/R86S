package org.example.r86s;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

final class ClientTraffic {
    private static final List<String> REGIONS = List.of("cn", "foreign", "unknown");
    private static final Set<String> ROUTES = Set.of("direct", "proxy", "unknown");
    private static final Set<String> SCOPES = Set.of("internet", "internal");
    private static final Set<String> CATEGORIES = Set.of("all", "aws", "cn");
    private static final Set<String> SAMPLE_STATES = Set.of("ok", "partial");
    private static final Set<String> INVALID_MACS = Set.of("00:00:00:00:00:00", "ff:ff:ff:ff:ff:ff");
    private static final Pattern IP = Pattern.compile("(?=.{2,45}$)(?:[0-9]{1,3}(?:\\.[0-9]{1,3}){3}|[0-9a-f.:]*:[0-9a-f.:]*)");
    private static final Pattern MAC = Pattern.compile("[0-9a-f]{2}(?::[0-9a-f]{2}){5}");
    private static final Pattern HOSTNAME = Pattern.compile(
            "(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\\.)*[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?");
    private static final int DEVICE_LIMIT = 1_000;
    private static final int DESTINATION_LIMIT = 250;
    private static final int TRACKED_DESTINATION_LIMIT = 50_000;
    private static final int DEVICE_SERVICE_LIMIT = 1_000;
    private static final long WEEK_MS = 7 * 86_400_000L;
    private static final long HOUR_MS = 3_600_000L;

    private ClientTraffic() {
    }

    record Filters(String scope, String region, String route, String client, String category) {
        static Filters from(Map<String, String> parameters) {
            String scope = parameters.getOrDefault("scope", "internet");
            String region = parameters.getOrDefault("region", "all");
            String route = parameters.getOrDefault("route", "all");
            String client = lower(parameters.get("client"));
            String category = parameters.getOrDefault("category", "all");
            if (!SCOPES.contains(scope)) {
                throw new IllegalArgumentException("The scope must be internet or internal");
            }
            if (!"all".equals(region) && !("internal".equals(scope) ? "internal".equals(region) : REGIONS.contains(region))) {
                throw new IllegalArgumentException("Invalid region for this scope");
            }
            if (!"all".equals(route) && !ROUTES.contains(route)) {
                throw new IllegalArgumentException("The route must be all, direct, proxy or unknown");
            }
            if (client != null && !MAC.matcher(client).matches() && !IP.matcher(client).matches()) {
                throw new IllegalArgumentException("Use a client ID from the device list");
            }
            if (!CATEGORIES.contains(category)) {
                throw new IllegalArgumentException("The category must be all, aws or cn");
            }
            return new Filters(scope, region, route, client, category);
        }

        boolean includesDevice(Flow flow) {
            return client == null || client.equals(flow.clientId()) || client.equals(flow.clientIp());
        }

        boolean includesRegion(Flow flow) {
            return "all".equals(region) || region.equals(flow.target().region());
        }

        boolean includesCategory(Flow flow) {
            return switch (category) {
                case "aws" -> flow.viaAws();
                case "cn" -> "cn".equals(flow.target().region());
                default -> true;
            };
        }

        boolean includesRoute(Flow flow) {
            return "all".equals(route) || route.equals(flow.target().route());
        }
    }

    record Response(String from, String to, int windowSeconds, String state, List<Window> windows,
                    List<Total> totals, List<Device> devices, List<Destination> destinations, Quality quality,
                    List<Service> services, List<DeviceService> deviceServices, Long awsRxBytes, Long awsTxBytes) {
    }

    record Window(String start, Long rxBytes, Long txBytes, int sampleCount) {
    }

    record Total(String region, Long rxBytes, Long txBytes) {
    }

    record Device(String id, String ip, String mac, long rxBytes, long txBytes, String deviceName) {
    }

    record Destination(String ip, Integer port, String name, String nameSource, String protocol, String region, String route,
                       long rxBytes, long txBytes) {
    }

    record Service(String name, long rxBytes, long txBytes) {
    }

    record DeviceService(String deviceId, String ip, String mac, String name, long rxBytes, long txBytes,
                         List<String> targets, String deviceName, List<String> targetIps, boolean targetIpsTruncated) {
    }

    record AwsUsage(String from, String to, String state, Long rxBytes, Long txBytes, Long totalBytes,
                    int coveredSamples, int partialSamples, int counterSamples, int estimatedSamples) {
    }

    record Quality(int coveredSamples, int partialSamples, int missingWindows, boolean devicesTruncated,
                   boolean destinationsTruncated, boolean deviceServicesTruncated) {
    }

    static final class Accumulator {
        private final long from;
        private final long to;
        private final long windowMs;
        private final Filters filter;
        private final TrafficServices classifier;
        private final Map<Long, Bucket> windows = new HashMap<>();
        private final Map<String, Bytes> totals = new LinkedHashMap<>();
        private final Map<String, DeviceBytes> devices = new HashMap<>();
        private final Map<Target, Bytes> destinations = new HashMap<>();
        private final Map<String, String> serviceNames = new HashMap<>();
        private final Map<String, Bytes> services = new HashMap<>();
        private final Map<ServiceKey, DeviceBytes> deviceServices = new HashMap<>();
        private final Bytes aws = new Bytes();
        private int coveredSamples;
        private int partialSamples;
        private boolean devicesTruncated;
        private boolean destinationsTruncated;
        private boolean deviceServicesTruncated;

        Accumulator(long from, long to, Filters filter, TrafficServices classifier) {
            this.from = from;
            this.to = to;
            this.windowMs = to - from > WEEK_MS ? HOUR_MS : HistoryData.WINDOW_MS;
            this.filter = filter;
            this.classifier = classifier;
            List<String> regions = "internal".equals(filter.scope()) ? List.of("internal") : REGIONS;
            regions.forEach(region -> totals.put(region, new Bytes()));
        }

        void accept(long time, JsonNode document) {
            if (!usableSample(document)) {
                return;
            }

            coveredSamples++;
            Bucket window = windows.computeIfAbsent(windowStart(time), ignored -> new Bucket());
            window.samples++;
            boolean partial = partialSample(document);
            for (JsonNode entry : document.path("flows")) {
                Flow flow = Flow.from(entry);
                if (flow == null) {
                    partial = true;
                } else if (filter.scope().equals(flow.scope())) {
                    include(flow, window);
                }
            }
            if (partial) {
                partialSamples++;
            }
        }

        private void include(Flow flow, Bucket window) {
            boolean routeMatches = filter.includesRoute(flow);
            boolean regionMatches = filter.includesRegion(flow);
            boolean deviceMatches = filter.includesDevice(flow);

            // Region totals stay visible across region filters. Device choices stay visible across device filters.
            if (deviceMatches) {
                if (flow.viaAws()) {
                    aws.add(flow);
                }
                if (routeMatches) {
                    totals.get(flow.target().region()).add(flow);
                }
            }
            if (regionMatches && routeMatches) {
                DeviceBytes device = devices.get(flow.clientId());
                if (device == null && (devices.size() < DEVICE_LIMIT || deviceMatches && filter.client() != null)) {
                    device = new DeviceBytes(flow);
                    devices.put(flow.clientId(), device);
                }
                if (device != null) {
                    device.add(flow);
                } else {
                    devicesTruncated = true;
                }
            }
            if (deviceMatches && regionMatches && routeMatches && filter.includesCategory(flow)) {
                window.bytes.add(flow);
                includeService(flow);
                Bytes destination = destinations.get(flow.target());
                if (destination == null && destinations.size() < TRACKED_DESTINATION_LIMIT) {
                    destination = new Bytes();
                    destinations.put(flow.target(), destination);
                }
                if (destination == null) {
                    destinationsTruncated = true;
                } else {
                    destination.add(flow);
                }
            }
        }

        private void includeService(Flow flow) {
            Target target = flow.target();
            String name = classifier.identify(target.ip(), target.name(), target.nameSource(), serviceNames);
            services.computeIfAbsent(name, ignored -> new Bytes()).add(flow);

            var key = new ServiceKey(flow.clientId(), name);
            DeviceBytes device = deviceServices.get(key);
            if (device == null && deviceServices.size() < TRACKED_DESTINATION_LIMIT) {
                device = new DeviceBytes(flow);
                deviceServices.put(key, device);
            }
            if (device == null) {
                deviceServicesTruncated = true;
                return;
            }
            device.add(flow);
            if (device.targets.size() < 12) {
                device.targets.add(target.ip() + (target.name() == null ? "" : " · " + target.name()));
            }
            if (device.targetIps.size() < 12) {
                device.targetIps.add(target.ip());
            } else if (!device.targetIps.contains(target.ip())) {
                device.targetIpsTruncated = true;
            }
        }

        private long windowStart(long time) {
            return Math.floorDiv(time, windowMs) * windowMs;
        }

        Response response() {
            List<Window> series = new ArrayList<>();
            int missingWindows = 0;
            for (long start = windowStart(from); start < to; start += windowMs) {
                Bucket window = windows.get(start);
                if (window == null) {
                    series.add(new Window(HistoryData.iso(start), null, null, 0));
                    missingWindows++;
                } else {
                    series.add(new Window(HistoryData.iso(start), window.bytes.rx, window.bytes.tx, window.samples));
                }
            }
            String state = coveredSamples == 0 ? "unavailable"
                    : partialSamples > 0 || missingWindows > 0 ? "partial" : "available";
            List<Total> regionTotals = totals.entrySet().stream()
                    .map(e -> new Total(e.getKey(), coveredSamples == 0 ? null : e.getValue().rx,
                            coveredSamples == 0 ? null : e.getValue().tx)).toList();
            List<Device> deviceList = devices.entrySet().stream()
                    .sorted(Comparator.<Map.Entry<String, DeviceBytes>>comparingDouble(e -> e.getValue().bytes.rank())
                            .reversed().thenComparing(Map.Entry::getKey))
                    .map(e -> new Device(e.getKey(), e.getValue().ip, e.getValue().mac,
                            e.getValue().bytes.rx, e.getValue().bytes.tx,
                            deviceName(e.getValue()))).toList();
            List<Destination> destinationList = destinations.entrySet().stream()
                    .sorted(Comparator.<Map.Entry<Target, Bytes>>comparingDouble(e -> e.getValue().rank()).reversed()
                            .thenComparing(e -> e.getKey().ip())
                            .thenComparing(e -> e.getKey().port(), Comparator.nullsLast(Comparator.naturalOrder()))
                            .thenComparing(e -> e.getKey().name(), Comparator.nullsLast(Comparator.naturalOrder()))
                            .thenComparing(e -> e.getKey().nameSource(), Comparator.nullsLast(Comparator.naturalOrder()))
                            .thenComparing(e -> e.getKey().protocol(), Comparator.nullsLast(Comparator.naturalOrder()))
                            .thenComparing(e -> e.getKey().region()).thenComparing(e -> e.getKey().route()))
                    .limit(DESTINATION_LIMIT)
                    .map(e -> {
                        Target target = e.getKey();
                        return new Destination(target.ip(), target.port(), target.name(), target.nameSource(), target.protocol(),
                                target.region(), target.route(), e.getValue().rx, e.getValue().tx);
                    }).toList();
            List<Service> serviceList = services.entrySet().stream()
                    .sorted(Comparator.<Map.Entry<String, Bytes>>comparingDouble(e -> e.getValue().rank())
                            .reversed().thenComparing(Map.Entry::getKey))
                    .map(e -> new Service(e.getKey(), e.getValue().rx, e.getValue().tx)).toList();
            List<DeviceService> deviceServiceList = deviceServices.entrySet().stream()
                    .sorted(Comparator.<Map.Entry<ServiceKey, DeviceBytes>>comparingDouble(e -> e.getValue().bytes.rank())
                            .reversed().thenComparing(e -> e.getKey().id()).thenComparing(e -> e.getKey().name()))
                    .limit(DEVICE_SERVICE_LIMIT)
                    .map(e -> new DeviceService(e.getKey().id(), e.getValue().ip, e.getValue().mac,
                            e.getKey().name(), e.getValue().bytes.rx, e.getValue().bytes.tx,
                            List.copyOf(e.getValue().targets), deviceName(e.getValue()),
                            List.copyOf(e.getValue().targetIps), e.getValue().targetIpsTruncated)).toList();
            return new Response(HistoryData.iso(from), HistoryData.iso(to), (int) (windowMs / 1_000), state,
                    series, regionTotals, deviceList,
                    destinationList, new Quality(coveredSamples, partialSamples, missingWindows, devicesTruncated,
                    destinationsTruncated || destinations.size() > DESTINATION_LIMIT,
                    deviceServicesTruncated || deviceServices.size() > DEVICE_SERVICE_LIMIT),
                    serviceList, deviceServiceList, coveredSamples == 0 ? null : aws.rx, coveredSamples == 0 ? null : aws.tx);
        }

        private String deviceName(DeviceBytes device) {
            String name = classifier.deviceName(device.mac, device.ip);
            return name != null ? name : device.router ? "路由器" : device.hostname;
        }
    }

    static final class AwsAccumulator {
        private final long from;
        private final long to;
        private final Bytes bytes = new Bytes();
        private final Set<Long> windows = new java.util.HashSet<>();
        private int coveredSamples;
        private int partialSamples;
        private int counterSamples;
        private int estimatedSamples;

        AwsAccumulator(long from, long to) {
            this.from = from;
            this.to = to;
        }

        void accept(long time, JsonNode document, JsonNode proxy) {
            if (proxy != null && proxy.isObject()) {
                Long rx = JsonValues.integer(proxy.path("rx_bytes"));
                Long tx = JsonValues.integer(proxy.path("tx_bytes"));
                if (rx != null && tx != null && rx >= 0 && tx >= 0) {
                    windows.add(HistoryData.windowStart(time));
                    coveredSamples++;
                    counterSamples++;
                    if (proxy.path("partial").asBoolean()) partialSamples++;
                    bytes.add(rx, tx);
                    return;
                }
            }
            if (!usableSample(document)) {
                return;
            }
            windows.add(HistoryData.windowStart(time));
            coveredSamples++;
            estimatedSamples++;
            partialSamples++;
            for (JsonNode node : document.path("flows")) {
                if ("proxy".equals(node.path("route").asText())) {
                    Flow flow = Flow.from(node);
                    if (flow != null && flow.viaAws()) {
                        bytes.add(flow);
                    }
                }
            }
        }

        AwsUsage result() {
            boolean available = coveredSamples > 0;
            long expected = (HistoryData.windowStart(to - 1) - HistoryData.windowStart(from)) / HistoryData.WINDOW_MS + 1;
            String state = !available ? "unavailable" : partialSamples > 0 || windows.size() < expected ? "partial" : "available";
            return new AwsUsage(HistoryData.iso(from), HistoryData.iso(to), state,
                    available ? bytes.rx : null, available ? bytes.tx : null,
                    available ? Math.addExact(bytes.rx, bytes.tx) : null, coveredSamples, partialSamples,
                    counterSamples, estimatedSamples);
        }
    }

    private static boolean usableSample(JsonNode document) {
        return document != null && document.isObject() && SAMPLE_STATES.contains(document.path("state").asText())
                && document.path("flows").isArray();
    }

    private static boolean partialSample(JsonNode document) {
        return "partial".equals(document.path("state").asText()) || document.path("partial").asBoolean();
    }

    private record ServiceKey(String id, String name) {
    }

    private record Target(String ip, Integer port, String name, String nameSource, String protocol, String region, String route) {
    }

    private record Flow(String scope, String clientIp, String clientMac, String clientHostname, Target target,
                        long rx, long tx) {
        static Flow from(JsonNode row) {
            String scope = row.path("scope").asText();
            String clientIp = ip(row.path("client_ip").textValue());
            String targetIp = ip(row.path("target_ip").textValue());
            Long rx = JsonValues.integer(row.path("rx_bytes"));
            Long tx = JsonValues.integer(row.path("tx_bytes"));
            if (!SCOPES.contains(scope) || clientIp == null || targetIp == null
                    || rx == null || tx == null || rx < 0 || tx < 0) {
                return null;
            }

            String clientMac = lower(row.path("client_mac").textValue());
            if (clientMac != null && (!MAC.matcher(clientMac).matches()
                    || INVALID_MACS.contains(clientMac))) {
                clientMac = null;
            }
            Long rawPort = JsonValues.integer(row.path("target_port"));
            Integer port = rawPort == null || rawPort < 1 || rawPort > 65_535 ? null : rawPort.intValue();
            String name = boundedText(row.path("target_name").textValue(), 253);
            String nameSource = name == null ? null : boundedText(row.path("target_name_source").textValue(), 16);
            String protocol = boundedText(row.path("protocol").textValue(), 16);
            String region = row.path("region").asText();
            if ("internal".equals(scope)) {
                region = "internal";
            } else if (!REGIONS.contains(region)) {
                region = "unknown";
            }
            String route = row.path("route").asText();
            if (!ROUTES.contains(route)) {
                route = "unknown";
            }
            String clientHostname = hostname(row.path("client_hostname").textValue());
            return new Flow(scope, clientIp, clientMac, clientHostname,
                    new Target(targetIp, port, name, nameSource, protocol, region, route), rx, tx);
        }

        String clientId() {
            return clientMac == null ? clientIp : clientMac;
        }

        boolean viaAws() {
            return "internet".equals(scope) && "proxy".equals(target.route());
        }
    }

    private static String ip(String value) {
        value = lower(value);
        return value != null && IP.matcher(value).matches() ? value : null;
    }

    private static String lower(String value) {
        return value == null ? null : value.toLowerCase(Locale.ROOT);
    }

    private static String hostname(String value) {
        if (value == null) {
            return null;
        }
        String name = value.endsWith(".") ? value.substring(0, value.length() - 1) : value;
        return name.length() <= 253 && HOSTNAME.matcher(name).matches() && ip(name) == null ? name : null;
    }

    private static String boundedText(String value, int maxLength) {
        if (value == null || value.isBlank() || value.length() > maxLength
                || value.codePoints().anyMatch(Character::isISOControl)) {
            return null;
        }
        return value;
    }

    private static final class Bytes {
        private long rx;
        private long tx;

        void add(Flow flow) {
            add(flow.rx(), flow.tx());
        }

        void add(long rxBytes, long txBytes) {
            rx = Math.addExact(rx, rxBytes);
            tx = Math.addExact(tx, txBytes);
        }

        double rank() {
            return (double) rx + tx;
        }
    }

    private static final class Bucket {
        private int samples;
        private final Bytes bytes = new Bytes();
    }

    private static final class DeviceBytes {
        private String ip;
        private final String mac;
        private final boolean router;
        private String hostname;
        private final Bytes bytes = new Bytes();
        private final Set<String> targets = new LinkedHashSet<>();
        private final Set<String> targetIps = new LinkedHashSet<>();
        private boolean targetIpsTruncated;

        DeviceBytes(Flow flow) {
            this.ip = flow.clientIp();
            this.mac = flow.clientMac();
            this.router = "tunnel".equals(flow.target().nameSource());
        }

        void add(Flow flow) {
            ip = flow.clientIp();
            if (flow.clientHostname() != null) {
                hostname = flow.clientHostname();
            }
            bytes.add(flow);
        }
    }
}

