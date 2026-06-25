package smartgrid.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class DistrictNodeManager {

    private static final String SERVER_ADDR = "localhost:9092";
    private static final String USER_EVENTS_TOPIC = "user-events";
    private static final String NODE_EVENTS_TOPIC = "node-events";

    private final Set<String> validUsers = new HashSet<>();
    private final Map<String, GridNode> nodes = new HashMap<>();
    private final Gson gson = new Gson();

    public static class GridNode {
        public String nodeId, districtId, type, ownerId;
        public double capacity;
        public GridNode(String nodeId, String districtId, String type, double capacity, String ownerId) {
            this.nodeId = nodeId; this.districtId = districtId; this.type = type;
            this.capacity = capacity; this.ownerId = ownerId;
        }
    }

    public static class NodeEvent {
        public String type, nodeId, districtId, nodeType, ownerId;
        public double capacity;
        public long timestamp;
        public NodeEvent(String type, String nodeId, String districtId,
                         String nodeType, double capacity, String ownerId) {
            this.type = type; this.nodeId = nodeId; this.districtId = districtId;
            this.nodeType = nodeType; this.capacity = capacity; this.ownerId = ownerId;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // ── Fault Recovery con assign+seek ───────────────────────────────────────

    private void recoverState() {
        System.out.println("[DistrictNodeManager] Recuperando estado...");
        recoverFromTopic(USER_EVENTS_TOPIC, record -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> event = gson.fromJson(record.value(), Map.class);
            String eventType = (String) event.get("type");
            String userId = (String) event.get("userId");
            if ("UserRegistered".equals(eventType) || "UserUpdated".equals(eventType))
                validUsers.add(userId);
            else if ("UserDeleted".equals(eventType))
                validUsers.remove(userId);
        });
        recoverFromTopic(NODE_EVENTS_TOPIC, record ->
                applyNodeEvent(gson.fromJson(record.value(), NodeEvent.class)));
        System.out.println("[DistrictNodeManager] Estado recuperado: "
                + validUsers.size() + " usuarios, " + nodes.size() + " nodos.");
    }

    private void recoverFromTopic(String topic,
            java.util.function.Consumer<ConsumerRecord<String, String>> handler) {

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dnm-recovery-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) return;

            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : partitions)
                tps.add(new TopicPartition(pi.topic(), pi.partition()));

            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            if (endOffsets.values().stream().noneMatch(o -> o > 0)) return;

            Map<TopicPartition, Long> current = new HashMap<>();
            tps.forEach(tp -> current.put(tp, 0L));

            while (!reachedEnd(current, endOffsets)) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.of(3, ChronoUnit.SECONDS));
                for (ConsumerRecord<String, String> record : records) {
                    handler.accept(record);
                    current.put(new TopicPartition(record.topic(), record.partition()),
                            record.offset() + 1);
                }
            }
        }
    }

    private boolean reachedEnd(Map<TopicPartition, Long> current, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet())
            if (current.getOrDefault(e.getKey(), 0L) < e.getValue()) return false;
        return true;
    }

    private void applyNodeEvent(NodeEvent event) {
        switch (event.type) {
            case "NodeCreated": case "NodeUpdated":
                nodes.put(event.nodeId, new GridNode(event.nodeId, event.districtId,
                        event.nodeType, event.capacity, event.ownerId)); break;
            case "NodeDeleted": nodes.remove(event.nodeId); break;
        }
    }

    // ── Producer ─────────────────────────────────────────────────────────────

    private KafkaProducer<String, String> createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void publishNodeEvent(NodeEvent event) {
        try (KafkaProducer<String, String> p = createProducer()) {
            p.send(new ProducerRecord<>(NODE_EVENTS_TOPIC, event.nodeId, gson.toJson(event)),
                (m, ex) -> { if (ex != null) System.err.println(ex.getMessage());
                             else System.out.println("[DistrictNodeManager] Evento publicado -> offset=" + m.offset()); });
            p.flush();
        }
    }

    // ── Comandos ─────────────────────────────────────────────────────────────

    public void createNode(String userId, String nodeId, String districtId, String type, double capacity) {
        if (!validUsers.contains(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: Usuario '" + userId + "' no registrado."); return; }
        if (nodes.containsKey(nodeId)) {
            System.out.println("[DistrictNodeManager] ERROR: Nodo '" + nodeId + "' ya existe."); return; }
        if (!isValidType(type)) {
            System.out.println("[DistrictNodeManager] ERROR: Tipo debe ser PRODUCER, CONSUMER o ACCUMULATOR."); return; }
        NodeEvent event = new NodeEvent("NodeCreated", nodeId, districtId, type, capacity, userId);
        publishNodeEvent(event);
        applyNodeEvent(event);
        System.out.println("[DistrictNodeManager] Nodo creado: " + nodeId + " (" + type + ") en distrito " + districtId);
    }

    public void updateNode(String userId, String nodeId, String districtId, String type, double capacity) {
        if (!validUsers.contains(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: Usuario '" + userId + "' no registrado."); return; }
        if (!nodes.containsKey(nodeId)) {
            System.out.println("[DistrictNodeManager] ERROR: Nodo '" + nodeId + "' no existe."); return; }
        if (!nodes.get(nodeId).ownerId.equals(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: No eres propietario del nodo."); return; }
        NodeEvent event = new NodeEvent("NodeUpdated", nodeId, districtId, type, capacity, userId);
        publishNodeEvent(event);
        applyNodeEvent(event);
        System.out.println("[DistrictNodeManager] Nodo actualizado: " + nodeId);
    }

    public void deleteNode(String userId, String nodeId) {
        if (!validUsers.contains(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: Usuario '" + userId + "' no registrado."); return; }
        if (!nodes.containsKey(nodeId)) {
            System.out.println("[DistrictNodeManager] ERROR: Nodo '" + nodeId + "' no existe."); return; }
        if (!nodes.get(nodeId).ownerId.equals(userId)) {
            System.out.println("[DistrictNodeManager] ERROR: No eres propietario del nodo."); return; }
        NodeEvent event = new NodeEvent("NodeDeleted", nodeId, null, null, 0, userId);
        publishNodeEvent(event);
        applyNodeEvent(event);
        System.out.println("[DistrictNodeManager] Nodo eliminado: " + nodeId);
    }

    public void listNodes() {
        if (nodes.isEmpty()) System.out.println("[DistrictNodeManager] No hay nodos.");
        else {
            System.out.println("[DistrictNodeManager] Nodos (" + nodes.size() + "):");
            nodes.values().forEach(n -> System.out.println(
                "  - " + n.nodeId + " | " + n.type + " | distrito=" + n.districtId
                + " | cap=" + n.capacity + " | owner=" + n.ownerId));
        }
    }

    private boolean isValidType(String type) {
        return "PRODUCER".equals(type) || "CONSUMER".equals(type) || "ACCUMULATOR".equals(type);
    }

    public static void main(String[] args) {
        DistrictNodeManager service = new DistrictNodeManager();
        service.recoverState();
        if (args.length == 0) { printUsage(); return; }
        switch (args[0]) {
            case "create": if (args.length < 5) { printUsage(); return; }
                service.createNode(args[1], args[2], args[3], args[4],
                        args.length >= 6 ? Double.parseDouble(args[5]) : 0.0); break;
            case "update": if (args.length < 5) { printUsage(); return; }
                service.updateNode(args[1], args[2], args[3], args[4],
                        args.length >= 6 ? Double.parseDouble(args[5]) : 0.0); break;
            case "delete": if (args.length < 3) { printUsage(); return; }
                service.deleteNode(args[1], args[2]); break;
            case "list": service.listNodes(); break;
            default: printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Uso:");
        System.out.println("  java DistrictNodeManager create <userId> <nodeId> <districtId> <PRODUCER|CONSUMER|ACCUMULATOR> [capacity]");
        System.out.println("  java DistrictNodeManager update <userId> <nodeId> <districtId> <type> [capacity]");
        System.out.println("  java DistrictNodeManager delete <userId> <nodeId>");
        System.out.println("  java DistrictNodeManager list");
    }
}