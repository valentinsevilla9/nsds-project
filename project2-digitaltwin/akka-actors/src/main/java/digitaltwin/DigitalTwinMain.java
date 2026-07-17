package digitaltwin;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiFunction;

// Arranca los actores del digital twin y expone una API HTTP sencilla
// para que Node-RED pueda hablar con ellos.
// Node-RED nos manda eventos (cambio de padre, crash...) por POST, y aqui los traducimos al
// mensaje de Akka que le toca a cada actor.
//
// Endpoints (puerto 8080 por defecto):
// POST /parent-update {"nodeId":"1","newParentId":"2"}
// POST /app-msg        {"fromNodeId":"1","seqNum":42}
// POST /set-period      {"nodeId":"1","periodMs":5000}
// POST /node-crash      {"nodeId":"1"}
// POST /node-recovered  {"nodeId":"1"}
// GET  /status          lista de nodos activos
//
// Uso: java DigitalTwinMain [numNodes] [httpPort] [remoteHost:remotePort]
//
// El tercer argumento es opcional. Sin el, todos los actores se crean
// aqui mismo (igual que antes). Si se da, la primera mitad de los nodos
// (incluido el root) se queda local y la segunda mitad se crea de
// verdad en el proceso RemoteNodeHost que corra en esa direccion - los
// mensajes hacia esos actores viajan por red via Akka Remoting
// (ActorSelection), no por una llamada local.
public class DigitalTwinMain {

    // los actores que viven aqui mismo, en este proceso
    private static final Map<String, ActorRef> localActors = new HashMap<>();
    // referencias a actores que viven en el otro portatil (RemoteNodeHost)
    private static final Map<String, ActorSelection> remoteActors = new HashMap<>();
    private static ActorSystem system;

    public static void main(String[] args) throws IOException, InterruptedException {
        int numNodes = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        int httpPort = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        String remoteAddress = args.length > 2 ? args[2] : null; // ej. "100.124.21.50:25520"

        System.out.println("=== Digital Twin for IoT Network ===");
        System.out.println("Nodes:     " + numNodes);
        System.out.println("HTTP port: " + httpPort);
        if (remoteAddress != null)
            System.out.println("Remote host for half the actors: " + remoteAddress);
        System.out.println("\n");

        system = ActorSystem.create("DigitalTwin", ConfigFactory.load());

        // sin remoteAddress: todos locales, igual que siempre. Con
        // remoteAddress: la primera mitad (con el root dentro) se queda
        // aqui, la segunda mitad vive de verdad en el otro proceso
        int localUpTo = (remoteAddress != null) ? (numNodes + 1) / 2 : numNodes;

        for (int i = 1; i <= numNodes; i++) {
            String nodeId = String.valueOf(i);
            String initialParent = (i == 1) ? "root" : "1";

            if (i <= localUpTo) {
                ActorRef actor = system.actorOf(
                        IoTNodeActor.props(nodeId, initialParent, 4000),
                        "node-" + nodeId);
                localActors.put(nodeId, actor);
                System.out.println("Created LOCAL actor for node " + nodeId +
                        " (parent=" + initialParent + ", period=4000ms)");
            } else {
                String remotePath = "akka://DigitalTwin@" + remoteAddress + "/user/node-" + nodeId;
                remoteActors.put(nodeId, system.actorSelection(remotePath));
                System.out.println("Wired REMOTE actor for node " + nodeId + " -> " + remotePath);
            }
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/parent-update", new ParentUpdateHandler());
        server.createContext("/app-msg", new AppMsgHandler());
        server.createContext("/set-period", new SetPeriodHandler());
        server.createContext("/node-crash", new NodeCrashHandler());
        server.createContext("/node-recovered", new NodeRecoveredHandler());
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

    // Los 4 handlers de los nodos son prácticamente iguales
    // (leer el body, sacar el nodeId, buscar el actor, mandarle un mensaje),
    // asi que comparten esta funcion. No importa si el actor es local o
    // remoto, un ActorSelection se usa con tell() exactamente igual que
    // un ActorRef - por eso el resto del codigo no tiene que saber la
    // diferencia.
    private static void handleNodeEvent(HttpExchange exchange, BiFunction<String, String, Object> messageFactory)
            throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) return;

        String body = readBody(exchange);
        String nodeId = extractField(body, "nodeId");
        Object msg = messageFactory.apply(nodeId, body);

        if (localActors.containsKey(nodeId)) {
            localActors.get(nodeId).tell(msg, ActorRef.noSender());
        } else if (remoteActors.containsKey(nodeId)) {
            remoteActors.get(nodeId).tell(msg, ActorRef.noSender());
        } else {
            respond(exchange, 404, "Node not found: " + nodeId);
            return;
        }
        respond(exchange, 200, "OK");
    }

    static class ParentUpdateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleNodeEvent(exchange, (nodeId, body) -> new ParentUpdateMsg(nodeId, extractField(body, "newParentId")));
        }
    }

    static class SetPeriodHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleNodeEvent(exchange, (nodeId, body) -> new SetPeriodMsg(Integer.parseInt(extractField(body, "periodMs"))));
        }
    }

    static class NodeCrashHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleNodeEvent(exchange, (nodeId, body) -> new NodeCrashMsg(nodeId));
        }
    }

    static class NodeRecoveredHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleNodeEvent(exchange, (nodeId, body) -> new NodeRecoveredMsg(nodeId));
        }
    }

    // Este o trae un nodeId propio, trae fromNodeId (quien mando el mensaje).
    // En la red real todo el trafico de aplicacion converge en el root, asi que aqui hacemos
    // lo mismo y se lo mandamos solo al actor del root ("1"), en vez de a todos los actores.
    // El root ("1") siempre cae en la mitad local por construccion, asi
    // que aqui no hace falta mirar el mapa de remotos.
    static class AppMsgHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) return;

            String body = readBody(exchange);
            String fromNodeId = extractField(body, "fromNodeId");
            int seqNum = Integer.parseInt(extractField(body, "seqNum"));

            ActorRef root = localActors.get("1");
            if (root != null) {
                root.tell(new AppMsg(fromNodeId, seqNum), ActorRef.noSender());
            }
            respond(exchange, 200, "OK");
        }
    }

    static class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            StringBuilder sb = new StringBuilder("{\"nodes\":[");
            boolean first = true;
            for (String nodeId : localActors.keySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(nodeId).append("\" (local)");
                first = false;
            }
            for (String nodeId : remoteActors.keySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(nodeId).append("\" (remote)");
                first = false;
            }
            sb.append("]}");
            respond(exchange, 200, sb.toString());
        }
    }

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

    // parser de JSON casero, sin libreria externa, porque los payloads
    // que nos llegan son siempre planos: {"campo":"valor", ...}
    private static String extractField(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0)
            return "";
        idx += key.length();
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
