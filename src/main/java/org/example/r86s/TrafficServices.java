package org.example.r86s;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class TrafficServices {
    private static final String UNKNOWN = "未知";
    private static final String HUAWEI_ECS_SUFFIX = ".compute.hwclouds-dns.com";

    // Reverse DNS is classified separately, never as evidence for an application.
    private static final List<Service> CATALOG = List.of(
            new Service("YouTube", "youtube.com", "youtu.be", "googlevideo.com", "ytimg.com", "youtube-nocookie.com"),
            new Service("Google", "google.com", "google.com.hk", "google.co.jp", "gstatic.com"),
            new Service("淘宝", "taobao.com", "tmall.com"),
            new Service("支付宝", "alipay.com"),
            new Service("京东", "jd.com", "360buy.com"),
            new Service("哔哩哔哩", "bilibili.com", "bilivideo.com", "hdslb.com"),
            new Service("抖音", "douyin.com", "douyinvod.com"),
            new Service("微信", "weixin.qq.com", "wechat.com", "weixin.com"),
            new Service("腾讯视频", "v.qq.com"),
            new Service("小红书", "xiaohongshu.com", "xhscdn.com"),
            new Service("百度", "baidu.com"),
            new Service("ChatGPT", "chatgpt.com", "openai.com", "oaistatic.com", "oaiusercontent.com"),
            new Service("Apple", "apple.com", "icloud.com", "mzstatic.com"),
            new Service("Microsoft", "microsoft.com", "windowsupdate.com", "windows.com"),
            new Service("GitHub", "github.com", "githubusercontent.com", "githubassets.com"),
            new Service("Netflix", "netflix.com", "nflxvideo.net", "nflximg.net"),
            new Service("Steam", "steampowered.com", "steamcommunity.com", "steamstatic.com"),
            new Service("Telegram", "telegram.org", "t.me", "telegram.me"),
            new Service("TikTok", "tiktok.com", "tiktokcdn.com"),
            new Service("X", "x.com", "twitter.com", "twimg.com"),
            new Service("Discord", "discord.com", "discord.gg", "discordapp.com"),
            new Service("Instagram", "instagram.com", "cdninstagram.com"),
            new Service("Facebook", "facebook.com", "fbcdn.net"),
            new Service("Cloudflare Tunnel", "argotunnel.com"));

    // These are provider-operated cloud endpoints, rather than general company or app domains.
    private static final List<Service> HOSTING = List.of(
            new Service("华为云（托管）", "myhuaweicloud.com"),
            new Service("百度智能云（托管）", "baidubce.com", "bcebos.com"),
            new Service("阿里云（托管）", "aliyuncs.com"),
            new Service("腾讯云（托管）", "myqcloud.com", "tencentcloudapi.com", "tencentcos.cn"),
            new Service("火山引擎（托管）", "volces.com", "volcengineapi.com"),
            new Service("UCloud（托管）", "ufileos.com", "api.ucloud.cn"),
            new Service("京东云（托管）", "jdcloud-api.com"),
            new Service("AWS（托管）", "amazonaws.com", "amazonaws.com.cn", "cloudfront.net"),
            new Service("Google Cloud（托管）", "bc.googleusercontent.com", "cloud.goog", "appspot.com", "run.app"),
            new Service("Azure（托管）", "cloudapp.azure.com", "cloudapp.net", "azurewebsites.net", "blob.core.windows.net"),
            new Service("Oracle Cloud（托管）", "oraclecloud.com", "customer-oci.com"));

    private final Rules manual;
    private final Rules geosite;
    private final Map<String, String> namesByMac = new HashMap<>();
    private final Map<String, String> namesByIp = new HashMap<>();

    TrafficServices(Path state, List<TrafficRule> rules) {
        manual = new Rules(new HashMap<>(), new HashMap<>(), new HashMap<>());
        for (TrafficRule rule : rules) {
            switch (rule.kind()) {
                case "ip" -> manual.ips().put(rule.target(), rule.label());
                case "domain" -> manual.domains().put(rule.target(), rule.label());
                case "exact" -> manual.exact().put(rule.target(), rule.label());
                case "mac" -> namesByMac.put(rule.target(), rule.label());
                case "client_ip" -> namesByIp.put(rule.target(), rule.label());
            }
        }
        geosite = readGeoSite(state);
    }

    String deviceName(String mac, String address) {
        String name = mac == null ? null : namesByMac.get(mac);
        return name != null ? name : lookupIp(namesByIp, address);
    }

    String identify(String address, String target, String source, Map<String, String> cache) {
        String ipOverride = lookupIp(manual.ips(), address);
        if (ipOverride != null) return ipOverride;
        if (target == null) return UNKNOWN;
        String name = target.toLowerCase(Locale.ROOT);
        if (name.endsWith(".")) {
            name = name.substring(0, name.length() - 1);
        }
        if ("ptr".equals(source)) {
            // Require the provider's exact format and the IPv4 address encoded in its ECS name.
            if (name.endsWith(HUAWEI_ECS_SUFFIX)) {
                return address != null && address.indexOf(':') < 0
                        && name.equals("ecs-" + address.replace('.', '-') + HUAWEI_ECS_SUFFIX)
                        ? "华为云（托管）" : UNKNOWN;
            }
            return match(name, HOSTING);
        }
        if (!("xray".equals(source) || "dns".equals(source)
                || ("tunnel".equals(source) && "argotunnel.com".equals(name)))) {
            return UNKNOWN;
        }
        String cached = cache.get(name);
        if (cached != null) {
            return cached;
        }
        String service = domainMatch(manual, name);
        if (service == null) service = match(name, CATALOG);
        if (UNKNOWN.equals(service)) {
            String site = domainMatch(geosite, name);
            if (site != null) service = site;
        }
        if (UNKNOWN.equals(service)) service = match(name, HOSTING);
        if (cache.size() < 8_192) {
            cache.put(name, service);
        }
        return service;
    }

    private static String lookupIp(Map<String, String> rules, String address) {
        if (address == null || rules.isEmpty()) return null;
        String name = rules.get(address);
        if (name != null || address.indexOf(':') < 0) return name;
        try {
            return rules.get(TrafficRule.ip(address));
        } catch (IllegalArgumentException ignored) {
            // Historical data can include malformed IP literals.
            return null;
        }
    }

    private static String domainMatch(Rules rules, String name) {
        String exact = rules.exact().get(name);
        if (exact != null) return exact;
        for (String suffix = name; !suffix.isEmpty();) {
            String match = rules.domains().get(suffix);
            if (match != null) return match;
            int dot = suffix.indexOf('.');
            if (dot < 0) break;
            suffix = suffix.substring(dot + 1);
        }
        return null;
    }

    private static Rules readGeoSite(Path state) {
        var result = new Rules(new HashMap<>(), new HashMap<>(), new HashMap<>());
        if (state == null) return result;
        try {
            Path path = state.resolve("service-geosite.tsv");
            if (!Files.isRegularFile(path) || Files.size(path) > 1_048_576) return result;
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                if (line.isBlank() || line.startsWith("#")) continue;
                String[] columns = line.split("\t", 3);
                if (columns.length != 3) continue;
                String key = columns[1].strip().toLowerCase(Locale.ROOT).replaceFirst("\\.$", "");
                String service = columns[2].strip();
                if (key.isEmpty() || key.length() > 253 || service.isEmpty() || service.length() > 64
                        || "其他".equals(service) || "未知".equals(service)) continue;
                switch (columns[0].strip()) {
                    case "domain" -> result.domains().put(key, service);
                    case "exact" -> result.exact().put(key, service);
                    default -> { }
                }
            }
        } catch (IOException e) {
            System.err.println("Unable to load GeoSite service rules");
        }
        return result;
    }

    private static String match(String hostname, List<Service> catalog) {
        for (Service service : catalog) {
            for (String domain : service.domains()) {
                int suffixStart = hostname.length() - domain.length();
                if (suffixStart == 0 && hostname.equals(domain)
                        || suffixStart > 0 && hostname.charAt(suffixStart - 1) == '.' && hostname.endsWith(domain)) {
                    return service.name();
                }
            }
        }
        return UNKNOWN;
    }

    private record Service(String name, String... domains) {
    }

    private record Rules(Map<String, String> ips, Map<String, String> domains, Map<String, String> exact) {
    }
}
