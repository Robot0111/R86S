package org.example.r86s;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

final class EventClassifier {
    private static final Set<String> WARNINGS = Set.of(
            "warn", "warning", "err", "error", "crit", "alert", "emerg");
    private static final Pattern STORAGE = Pattern.compile(
            "i/o error|buffer i/o|blk_update_request|usb disconnect|ext4-fs error|"
            + "btrfs.*(?:error|corrupt)|f2fs.*(?:error|corrupt)|read-only file system|no space left on device");
    private static final Pattern PPP_UP = Pattern.compile(
            "pppd.*(?:local\\s+ip address|remote\\s+ip address)|interface 'wan' is now up|pppoe-wan.*link is up");
    private static final Pattern PPP_DOWN = Pattern.compile(
            "pppd.*(?:lcp.*(?:timeout|terminated)|connection terminated|authentication failed|"
            + "modem hangup|pado packets|pppoe.*(?:error|failed))|interface 'wan' (?:is now down|has lost the connection)");
    private static final Pattern AUTH_SUCCESS = Pattern.compile(
            "password auth succeeded|pubkey auth succeeded|accepted (?:publickey|password)|authentication succeeded");
    private static final Pattern AUTH_FAILURE = Pattern.compile(
            "bad password|failed (?:password|publickey|login)|login failed|authentication fail|invalid user");
    private static final Pattern DNS_LIFECYCLE = Pattern.compile(
            "(?:dnsmasq|smartdns|odhcpd).*?(?:started|starting|exiting|stopped|restarted|sigterm)");
    private static final Pattern DNS_ERROR = Pattern.compile(
            "no address available|no addresses available|address pool.*exhaust|failed to (?:send|resolve|bind)|"
            + "no servers found|upstream.*(?:error|timeout|failed)|dhcp packet received");

    private EventClassifier() {
    }

    static String type(String category, String severity, String raw) {
        if ("boot-snapshot".equals(category)) {
            return "boot";
        }
        if (raw == null) {
            return null;
        }
        String text = raw.toLowerCase(Locale.ROOT);
        boolean warning = WARNINGS.contains(severity);

        if (text.contains("dhcp packet received on eth2.644 which has no address")
                || text.contains(" dnsmasq-repair: r86s-dhcp-")) {
            return null;
        }
        if (text.contains("out of memory") || text.contains("oom-kill") || text.contains("oom_reaper")) {
            return "oom";
        }
        if (text.contains("conntrack") && text.contains("table full")) {
            return "conntrack_full";
        }
        if (STORAGE.matcher(text).find()) {
            return "storage_error";
        }
        if (PPP_DOWN.matcher(text).find()) {
            return "pppoe_down";
        }
        if (PPP_UP.matcher(text).find()) {
            return "pppoe_up";
        }
        if (text.contains("link is down") || text.contains("link down")) {
            return "link_down";
        }
        if (text.contains("link is up") || text.contains("link up")) {
            return "link_up";
        }

        return switch (category) {
            case "kernel" -> warning || text.contains("netdev watchdog") || text.contains("call trace:")
                    || text.contains("kernel panic") || text.contains("reset adapter") ? "kernel_fault" : null;
            case "network" -> warning ? "network_warning" : null;
            case "dns-dhcp" -> DNS_LIFECYCLE.matcher(text).find() ? "dns_service_change"
                    : warning || DNS_ERROR.matcher(text).find() ? "dns_dhcp_error" : null;
            case "authentication" -> AUTH_SUCCESS.matcher(text).find() ? "auth_success"
                    : AUTH_FAILURE.matcher(text).find() ? "auth_failure" : null;
            case "proxy-tunnel" -> warning || text.contains("failed to connect")
                    || text.contains("connection refused") || text.contains("connection timed out")
                    ? "tunnel_error" : null;
            case "firewall" -> warning ? "firewall_warning" : null;
            case "system" -> warning || text.contains("segfault") || text.contains("crash loop") ? "system_warning" : null;
            default -> null;
        };
    }
}
