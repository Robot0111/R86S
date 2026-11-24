package org.example.r86s;

import java.net.IDN;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

record TrafficRule(String id, String kind, String target, String label) {
    private static final Pattern RULE_ID = Pattern.compile("[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}");
    private static final Pattern MAC = Pattern.compile("[a-f0-9]{2}(?::[a-f0-9]{2}){5}");
    private static final Pattern IPV4 = Pattern.compile("[0-9]{1,3}(?:\\.[0-9]{1,3}){3}");
    private static final Pattern IPV6 = Pattern.compile("[a-f0-9.]*:[a-f0-9.:]*");

    static TrafficRule create(String id, String kind, String target, String label) {
        kind = kind.strip().toLowerCase(Locale.ROOT);
        target = target.strip().toLowerCase(Locale.ROOT);
        label = label.strip();
        if (label.isEmpty() || label.codePointCount(0, label.length()) > 64
                || label.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("名称须为 1 到 64 个字符，且不能包含控制字符");
        }
        if (("ip".equals(kind) || "domain".equals(kind) || "exact".equals(kind))
                && ("未知".equals(label) || "其他".equals(label))) {
            throw new IllegalArgumentException("请选择具体服务名");
        }
        String normalized = switch (kind) {
            case "ip", "client_ip" -> ip(target);
            case "domain", "exact" -> domain(target);
            case "mac" -> mac(target);
            default -> throw new IllegalArgumentException("规则类型必须是 ip、domain、exact、mac 或 client_ip");
        };
        return new TrafficRule(id == null ? UUID.randomUUID().toString() : id, kind, normalized, label);
    }

    static boolean validId(String id) {
        return RULE_ID.matcher(id).matches();
    }

    static String ip(String value) {
        if (IPV4.matcher(value).matches() || IPV6.matcher(value).matches()) {
            try {
                // Literal-only patterns ensure this does not perform a DNS lookup.
                return InetAddress.getByName(value).getHostAddress();
            } catch (UnknownHostException ignored) {
                // Keep the same validation message for all invalid literals.
            }
        }
        throw new IllegalArgumentException("请输入完整 IPv4 或 IPv6 地址");
    }

    private static String domain(String value) {
        try {
            String ascii = IDN.toASCII(value, IDN.USE_STD3_ASCII_RULES).toLowerCase(Locale.ROOT).replaceFirst("\\.$", "");
            if (ascii.length() <= 253 && ascii.indexOf('.') > 0 && !IPV4.matcher(ascii).matches()) {
                return ascii;
            }
        } catch (IllegalArgumentException ignored) {
            // Provide the domain-specific validation message below.
        }
        throw new IllegalArgumentException("请输入完整域名，例如 example.com");
    }

    private static String mac(String value) {
        String normalized = value.replace('-', ':');
        if (MAC.matcher(normalized).matches()) {
            return normalized;
        }
        throw new IllegalArgumentException("请输入形如 aa:bb:cc:dd:ee:ff 的 MAC 地址");
    }

    record Page(List<TrafficRule> items) {
    }
}
