package digitaltwin;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * DigitalTwinMain — arranca el sistema de actores del Digital Twin
 * y expone una API HTTP para que Node-RED pueda comunicarse con los actores.
 *
 * Node-RED envía eventos (cambio de padre, crash, etc.) a este servidor HTTP,
 * que los traduce en mensajes Akka a los actores correspondientes.
 *
 * Endpoints HTTP (escucha en puerto 8080):
 * POST /parent-update {"nodeId":"1","newParentId":"2"}
 * POST /app-msg {"fromNodeId":"1","seqNum":42}
 * POST /set-period {"nodeId":"1","periodMs":5000}
 * POST /node-crash {"nodeId":"1"}
 * GET /status devuelve lista de nodos activos
 *
 * Uso: java DigitalTwinMain [numNodes] [httpPort]
 * numNodes: número de nodos IoT a simular (default: 4)
 * httpPort: puerto HTTP para Node-RED (default: 8080)
 */
public class DigitalTwinMain {

    private static final Map<String, ActorRef> nodeActors = new HashMap<>();
    private static ActorSystem system;

    public static void main(String[] args) throws IOException, InterruptedException {
        int numNodes = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        int httpPort = args.length > 1 ? Integer.parseInt(args[1]) : 8080;

        System.out.println("=== Digital Twin for IoT Network ===");
        System.out.println("Nodes:     " + numNodes);
        System.out.println("HTTP port: " + httpPort);
        System.out.println("====================================\n");

        system = ActorSystem.create("DigitalTwin");

        // Crear un actor por nodo IoT
        // El nodo 1 es el root (border router), los demás son nodos regulares
        // Estado inicial: todos conectados al root como padre
        for (int i = 1; i <= numNodes; i++) {
            String nodeId = String.valueOf(i);
            String initialParent = (i == 1) ? "root" : "1";
            ActorRef actor = system.actorOf(
                    IoTNodeActor.props(nodeId, initialParent, 4000),
                    "node-" + nodeId);
            nodeActors.put(nodeId, actor);
            System.out.println("Created actor for node " + nodeId +
                    " (parent=" + initialParent + ", period=4000ms)");
        }

        // Arrancar servidor HTTP para Node-RED
        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/parent-update", new ParentUpdateHandler());
        server.createContext("/app-msg", new AppMsgHandler());
        server.createContext("/set-period", new SetPeriodHandler());
        server.createContext("/node-crash", new NodeCrashHandler());
        server.createContext("/status", new StatusHandler());
        server.start();

        System.out.println("\nHTTP server ready on port " + httpPort);
        System.out.println("Node-RED can now send events to this server.");
        System.out.println("Press Enter to stop...");
        try (Scanner scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

        server.stop(0);
        system.terminate();
    }

    // ── HTTP Handlers ─────────────────────────────────────────────────────

    static class ParentUpdateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = readBody(exchange);
                String nodeId = extractField(body, "nodeId");
                String newParentId = extractField(body, "newParentId");

                ActorRef actor = nodeActors.get(nodeId);
                if (actor != null) {
                    actor.tell(new ParentUpdateMsg(nodeId, newParentId), ActorRef.noSender());
                    respond(exchange, 200, "OK");
                } else {
                    respond(exchange, 404, "Node not found: " + nodeId);
                }
            }
        }
    }

    static class AppMsgHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = readBody(exchange);
                String fromNodeId = extractField(body, "fromNodeId");
                int seqNum = Integer.parseInt(extractField(body, "seqNum"));

                // Enviar AppMsg a todos los actores (broadcast, simulando tráfico al root)
                for (ActorRef actor : nodeActors.values()) {
                    actor.tell(new AppMsg(fromNodeId, seqNum), ActorRef.noSender());
                }
                respond(exchange, 200, "OK");
            }
        }
    }

    static class SetPeriodHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = readBody(exchange);
                String nodeId = extractField(body, "nodeId");
                int periodMs = Integer.parseInt(extractField(body, "periodMs"));

                ActorRef actor = nodeActors.get(nodeId);
                if (actor != null) {
                    actor.tell(new SetPeriodMsg(periodMs), ActorRef.noSender());
                    respond(exchange, 200, "OK");
                } else {
                    respond(exchange, 404, "Node not found: " + nodeId);
                }
            }
        }
    }

    static class NodeCrashHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                String body = readBody(exchange);
                String nodeId = extractField(body, "nodeId");

                ActorRef actor = nodeActors.get(nodeId);
                if (actor != null) {
                    actor.tell(new NodeCrashMsg(nodeId), ActorRef.noSender());
                    respond(exchange, 200, "OK");
                } else {
                    respond(exchange, 404, "Node not found: " + nodeId);
                }
            }
        }
    }

    static class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            StringBuilder sb = new StringBuilder("{\"nodes\":[");
            boolean first = true;
            for (String nodeId : nodeActors.keySet()) {
                if (!first)
                    sb.append(",");
                sb.append("\"").append(nodeId).append("\"");
                first = false;
            }
            sb.append("]}");
            respond(exchange, 200, sb.toString());
        }
    }

    // ── Utilidades HTTP ───────────────────────────────────────────────────

    private static String readBody(HttpExchange exchange) throws IOException {
        InputStream is = exchange.getRequestBody();
        return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, bytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(bytes);
        os.close();
    }

    /**
     * Extrae un campo de un JSON simple sin librería externa.
     * Ejemplo: extractField({"nodeId":"1","newParentId":"2"}, "nodeId") -> "1"
     */
    private static String extractField(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0)
            return "";
        idx += key.length();
        // Saltar hasta el valor
        while (idx < json.length() && (json.charAt(idx) == ':' || json.charAt(idx) == ' '))
            idx++;
        if (idx >= json.length())
            return "";
        boolean quoted = json.charAt(idx) == '"';
        if (quoted)
            idx++;
        int start = idx;
        while (idx < json.length()
                && (quoted ? json.charAt(idx) != '"' : json.charAt(idx) != ',' && json.charAt(idx) != '}'))
            idx++;
        return json.substring(start, idx).trim();
    }
}