package org.example.r86s;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws IOException {
        AppConfig config = AppConfig.load(System.getenv());
        ObjectMapper json = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        StatusReader status = new StatusReader(config.monitorDir().resolve("state"), json);
        HistoryStore store = config.database() == null ? null : new HistoryStore(config.database(), config.hostId(), json);
        MonitorApi api = new MonitorApi(json, status, store);

        HttpServer server = HttpServer.create(new InetSocketAddress(config.bindAddress(), config.port()), 0);
        ExecutorService http = Executors.newVirtualThreadPerTaskExecutor();
        ScheduledExecutorService importer = Executors.newSingleThreadScheduledExecutor();
        server.setExecutor(http);
        server.createContext("/api/v1/", api::handle);
        if (store != null) {
            var job = new HistoryImporter(config, json, store);
            importer.scheduleWithFixedDelay(job::run, 0, config.importSeconds(), TimeUnit.SECONDS);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            importer.shutdownNow();
            server.stop(1);
            http.shutdown();
        }));
        server.start();
        System.out.printf("R86S status API listening on %s:%d%n", config.bindAddress(), server.getAddress().getPort());
    }
}
