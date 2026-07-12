package smartgrid.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// Este servicio deja a los usuarios crear/editar/borrar nodos (producers,
// consumers, accumulators) dentro de un distrito. 

// Para saber si un usuario puede hacer algo, necesitamos saber que existe - 
// y eso no lo sabemos nosotros, lo sabe AccountService. 
// En vez de preguntarle por HTTP (eso seria acoplarnos a el), 
// nos leemos su topic "user-events" y nos montamos nuestra propia copia (validUsers) 
// de quien esta registrado.
// Es "event-carried state transfer": copiamos el dato que necesitamos en
// vez de depender de que el otro servicio este vivo para preguntarselo.
public class DistrictNodeManager {

    private static final String SERVER_ADDR = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
    private static final String USER_EVENTS_TOPIC = "user-events";
    private static final String NODE_EVENTS_TOPIC = "node-events";

    private final Set<String> validUsers = new HashSet<>();
    private final Map<String, GridNode> nodes = new HashMap<>();
    private final Gson gson = new Gson();

    public static class GridNode {
        public String nodeId, districtId, type, ownerId;
        public double capacity;

        public GridNode(String nodeId, String districtId, String type, double capacity, String ownerId) {
            this.nodeId = nodeId;
            this.districtId = districtId;
            this.type = type;
            this.capacity = capacity;
            this.ownerId = ownerId;
        }
    }

    public static class NodeEvent {
        public String type, nodeId, districtId, nodeType, ownerId;
        public double capacity;
        public long timestamp;

        public NodeEvent(String type, String nodeId, String districtId,
                String nodeType, double capacity, String ownerId) {
            this.type = type;
            this.nodeId = nodeId;
            this.districtId = districtId;
            this.nodeType = nodeType;
            this.capacity = capacity;
            this.ownerId = ownerId;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // Al arrancar, nos leemos dos topics enteros:
    // user-events (para saber que usuarios son validos) y node-events (para
    // reconstruir los nodos que ya existian).
    // Los dos por separado, cada uno reconstruye su parte del estado.
    private void recoverState() {
        System.out.println("[DistrictNodeManager] Recovering state...");
        KafkaSupport.replayTopic(SERVER_ADDR, USER_EVENTS_TOPIC, record -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> event = gson.fromJson(record.value(), Map.class);
            String eventType = (String) event.get("type");
            String userId = (String) event.get("userId");
            if ("UserRegistered".equals(eventType) || "UserUpdated".equals(eventType))
                validUsers.add(userId);
            else if ("UserDeleted".equals(eventType))
                validUsers.remove(userId);
        });
        KafkaSupport.replayTopic(SERVER_ADDR, NODE_EVENTS_TOPIC,
                record -> applyNodeEvent(gson.fromJson(record.value(), NodeEvent.class)));
        System.out.println("[DistrictNodeManager] State recovered: "
                + validUsers.size() + " users, " + nodes.size() + " nodes.");
    }

    private void applyNodeEvent(NodeEvent event) {
        switch (event.type) {
            case "NodeCreated":
            case "NodeUpdated":
                nodes.put(event.nodeId, new GridNode(event.nodeId, event.districtId,
                        event.nodeType, event.capacity, event.ownerId));
                break;
            case "NodeDeleted":
                nodes.remove(event.nodeId);
                break;
        }
    }

    private void publishNodeEvent(NodeEvent event) {
        try (KafkaProducer<String, String> p = KafkaSupport.createProducer(SERVER_ADDR)) {
            p.send(new ProducerRecord<>(NODE_EVENTS_TOPIC, event.nodeId, gson.toJson(event)),
                    (m, ex) -> {
                        if (ex != null)
                            System.err.println(ex.getMessage());
                        else
                            System.out.println("[DistrictNodeManager] Event published -> offset=" + m.offset());
                    });
            p.flush();
        }
    }

    // valida que el usuario exista (via nuestra copia de user-events),
    // que el nodo no exista ya, y que el tipo sea uno de los tres validos
    public void createNode(String userId, String nodeId, String districtId, String type, double capacity) {
        if (!validUsers.contains(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: User '" + userId + "' not registered.");
            return;
        }
        if (nodes.containsKey(nodeId)) {
            System.out.println("[DistrictNodeManager] ERROR: Node '" + nodeId + "' already exists.");
            return;
        }
        if (!isValidType(type)) {
            System.out.println("[DistrictNodeManager] ERROR: Type must be PRODUCER, CONSUMER or ACCUMULATOR.");
            return;
        }
        NodeEvent event = new NodeEvent("NodeCreated", nodeId, districtId, type, capacity, userId);
        publishNodeEvent(event);
        applyNodeEvent(event);
        System.out
                .println("[DistrictNodeManager] Node created: " + nodeId + " (" + type + ") in district " + districtId);
    }

    // igual que createNode pero ademas comprueba que quien lo pide sea el dueño del
    // nodo,
    // no vale que cualquier usuario registrado toque el nodo de otro
    public void updateNode(String userId, String nodeId, String districtId, String type, double capacity) {
        if (!validUsers.contains(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: User '" + userId + "' not registered.");
            return;
        }
        if (!nodes.containsKey(nodeId)) {
            System.out.println("[DistrictNodeManager] ERROR: Node '" + nodeId + "' does not exist.");
            return;
        }
        if (!nodes.get(nodeId).ownerId.equals(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: You are not the owner of this node.");
            return;
        }
        NodeEvent event = new NodeEvent("NodeUpdated", nodeId, districtId, type, capacity, userId);
        publishNodeEvent(event);
        applyNodeEvent(event);
        System.out.println("[DistrictNodeManager] Node updated: " + nodeId);
    }

    public void deleteNode(String userId, String nodeId) {
        if (!validUsers.contains(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: User '" + userId + "' not registered.");
            return;
        }
        if (!nodes.containsKey(nodeId)) {
            System.out.println("[DistrictNodeManager] ERROR: Node '" + nodeId + "' does not exist.");
            return;
        }
        if (!nodes.get(nodeId).ownerId.equals(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: You are not the owner of this node.");
            return;
        }
        NodeEvent event = new NodeEvent("NodeDeleted", nodeId, null, null, 0, userId);
        publishNodeEvent(event);
        applyNodeEvent(event);
        System.out.println("[DistrictNodeManager] Node deleted: " + nodeId);
    }

    public void listNodes() {
        if (nodes.isEmpty())
            System.out.println("[DistrictNodeManager] No nodes.");
        else {
            System.out.println("[DistrictNodeManager] Nodes (" + nodes.size() + "):");
            nodes.values().forEach(n -> System.out.println(
                    "  - " + n.nodeId + " | " + n.type + " | district=" + n.districtId
                            + " | cap=" + n.capacity + " | owner=" + n.ownerId));
        }
    }

    private boolean isValidType(String type) {
        return "PRODUCER".equals(type) || "CONSUMER".equals(type) || "ACCUMULATOR".equals(type);
    }

    public static void main(String[] args) {
        DistrictNodeManager service = new DistrictNodeManager();
        service.recoverState();
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "create":
                if (args.length < 5) {
                    printUsage();
                    return;
                }
                service.createNode(args[1], args[2], args[3], args[4],
                        args.length >= 6 ? Double.parseDouble(args[5]) : 0.0);
                break;
            case "update":
                if (args.length < 5) {
                    printUsage();
                    return;
                }
                service.updateNode(args[1], args[2], args[3], args[4],
                        args.length >= 6 ? Double.parseDouble(args[5]) : 0.0);
                break;
            case "delete":
                if (args.length < 3) {
                    printUsage();
                    return;
                }
                service.deleteNode(args[1], args[2]);
                break;
            case "list":
                service.listNodes();
                break;
            default:
                printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println(
                "  java DistrictNodeManager create <userId> <nodeId> <districtId> <PRODUCER|CONSUMER|ACCUMULATOR> [capacity]");
        System.out.println("  java DistrictNodeManager update <userId> <nodeId> <districtId> <type> [capacity]");
        System.out.println("  java DistrictNodeManager delete <userId> <nodeId>");
        System.out.println("  java DistrictNodeManager list");
    }
}
