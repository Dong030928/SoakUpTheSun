package org.hgc.suts.gateway;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Real Spring AI transport with scripted native text/tool-call responses; never a live provider. */
final class ModelHttpFixture implements AutoCloseable {
    final Queue<JSONObject> messages = new ConcurrentLinkedQueue<>();
    final Queue<JSONObject> requests = new ConcurrentLinkedQueue<>();
    private final HttpServer server;

    ModelHttpFixture() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/chat/completions", exchange -> {
                requests.add(JSONUtil.parseObj(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                JSONObject message = messages.poll();
                if (message == null) message = new JSONObject().set("role", "assistant").set("content", "执行完成");
                String body = JSONUtil.toJsonStr(new JSONObject().set("choices", List.of(new JSONObject()
                        .set("index", 0).set("finish_reason", message.containsKey("tool_calls") ? "tool_calls" : "stop")
                        .set("message", message))));
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
        } catch (IOException ex) { throw new ExceptionInInitializerError(ex); }
    }

    String baseUrl() { return "http://127.0.0.1:" + server.getAddress().getPort(); }

    void reset(String reply) {
        messages.clear();
        requests.clear();
        text(reply);
    }

    void text(String reply) { messages.add(new JSONObject().set("role", "assistant").set("content", reply)); }

    void tool(String name, String arguments) {
        messages.add(new JSONObject().set("role", "assistant").set("content", "")
                .set("tool_calls", List.of(new JSONObject().set("id", "call-" + messages.size()).set("type", "function")
                        .set("function", new JSONObject().set("name", name).set("arguments", arguments)))));
    }

    @Override
    public void close() { server.stop(0); }
}
