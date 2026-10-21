package org.hgc.suts.gateway.rag;

import com.sun.net.httpserver.HttpServer;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

final class RagIndexHttpTestServer implements AutoCloseable {
    final List<Request> requests = new CopyOnWriteArrayList<>();
    volatile Function<Request, Reply> responder = request -> new Reply(200, "{}");
    private final HttpServer server;
    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    RagIndexHttpTestServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            try {
                byte[] incoming = exchange.getRequestBody().readAllBytes();
                JsonNode body = incoming.length == 0 ? null : RagIndexHttp.JSON.readTree(incoming);
                var request = new Request(exchange.getRequestMethod(), exchange.getRequestURI().toString(), body,
                        exchange.getRequestHeaders().getFirst("Authorization"));
                requests.add(request);
                Reply reply = responder.apply(request);
                byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status(), reply.chunked() ? 0 : bytes.length);
                if (reply.delayMs() > 0) Thread.sleep(reply.delayMs());
                exchange.getResponseBody().write(bytes);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (IOException ignored) {
                // A timeout or response-size rejection closes the client side intentionally.
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
    @Override public void close() { server.stop(0); executor.shutdownNow(); }
    record Request(String method, String path, JsonNode body, String authorization) { }
    record Reply(int status, String body, boolean chunked, long delayMs) {
        Reply(int status, String body) { this(status, body, false, 0); }
    }
}
