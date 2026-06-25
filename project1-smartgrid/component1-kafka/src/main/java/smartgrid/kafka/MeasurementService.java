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

public class MeasurementService {

    private static final String SERVER_ADDR = "localhost:9092";
    private static final String NODE_EVENTS_TOPIC = "node-events";
    private static final String MEASUREMENTS_TOPIC = "measurements";

    private final Map<String, NodeInfo> nodes = new HashMap<>();
    private final Gson gson = new Gson();
    private final Random random = new Random();

    public static class NodeInfo {
        public String nodeId, districtId, nodeType;
        public NodeInfo(String nodeId, String districtId, String nodeType) {
            this.nodeId = nodeId; this.districtId = districtId; this.nodeType = nodeType;
        }
    }

    public static class MeasurementEvent {
        public String type = "MeasurementReported";
        public String nodeId, nodeType, districtId;
        public double value;
        public long timestamp;
        public MeasurementEvent(String nodeId, String nodeType, String districtId, double value) {
            this.nodeId = nodeId; this.nodeType = nodeType;
            this.districtId = districtId; this.value = value;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // ── Fault Recovery con assign+seek ───────────────────────────────────────

    private void recoverState() {
        System.out.println("[MeasurementService] Recuperando nodos desde '" + NODE_EVENTS_TOPIC + "'...");

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ms-recovery-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(NODE_EVENTS_TOPIC);
            if (partitions == null || partitions.isEmpty()) {
                System.out.println("[MeasurementService] Nodos recuperados: 0"); return;
            }

            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : partitions)
                tps.add(new TopicPartition(pi.topic(), pi.partition()));

            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            if (endOffsets.values().stream().noneMatch(o -> o > 0)) {
                System.out.println("[MeasurementService] Nodos recuperados: 0"); return;
            }

            Map<TopicPartition, Long> current = new HashMap<>();
            tps.forEach(tp -> current.put(tp, 0L));

            while (!reachedEnd(current, endOffsets)) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.of(3, ChronoUnit.SECONDS));
                for (ConsumerRecord<String, String> record : records) {
                    applyNodeEvent(record.value());
                    current.put(new TopicPartition(record.topic(), record.partition()),
                            record.offset() + 1);
                }
            }
        }

        System.out.println("[MeasurementService] Nodos recuperados: " + nodes.size());
    }

    private boolean reachedEnd(Map<TopicPartition, Long> current, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet())
            if (current.getOrDefault(e.getKey(), 0L) < e.getValue()) return false;
        return true;
    }

    private void applyNodeEvent(String json) {
        @SuppressWarnings("unchecked")
        Map<String, Object> event = gson.fromJson(json, Map.class);
        String eventType = (String) event.get("type");
        String nodeId = (String) event.get("nodeId");
        switch (eventType) {
            case "NodeCreated": case "NodeUpdated":
                nodes.put(nodeId, new NodeInfo(nodeId,
                        (String) event.get("districtId"),
                        (String) event.get("nodeType"))); break;
            case "NodeDeleted": nodes.remove(nodeId); break;
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

    public void reportMeasurement(String nodeId, double value) {
        if (!nodes.containsKey(nodeId)) {
            System.out.println("[MeasurementService] ERROR: Nodo '" + nodeId + "' no existe."); return;
        }
        NodeInfo node = nodes.get(nodeId);
        MeasurementEvent event = new MeasurementEvent(nodeId, node.nodeType, node.districtId, value);
        try (KafkaProducer<String, String> p = createProducer()) {
            p.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, node.districtId, gson.toJson(event)),
                (m, ex) -> { if (ex != null) System.err.println(ex.getMessage());
                             else System.out.println("[MeasurementService] Medicion publicada -> nodo="
                                     + nodeId + " valor=" + value + " kW offset=" + m.offset()); });
            p.flush();
        }
    }

    public void simulate(int numMeasurements) {
        if (nodes.isEmpty()) {
            System.out.println("[MeasurementService] No hay nodos. Crea nodos primero."); return;
        }
        List<NodeInfo> nodeList = new ArrayList<>(nodes.values());
        System.out.println("[MeasurementService] Simulando " + numMeasurements + " mediciones...");
        try (KafkaProducer<String, String> p = createProducer()) {
            for (int i = 0; i < numMeasurements; i++) {
                NodeInfo node = nodeList.get(random.nextInt(nodeList.size()));
                double value = generateValue(node.nodeType);
                MeasurementEvent event = new MeasurementEvent(node.nodeId, node.nodeType, node.districtId, value);
                p.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, node.districtId, gson.toJson(event)));
                System.out.printf("  [%d/%d] %s (%s) -> %.2f kW%n",
                        i + 1, numMeasurements, node.nodeId, node.nodeType, value);
                try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            }
            p.flush();
        }
        System.out.println("[MeasurementService] Simulacion completada.");
    }

    private double generateValue(String nodeType) {
        switch (nodeType) {
            case "PRODUCER":    return 10 + random.nextDouble() * 90;
            case "CONSUMER":    return -(5 + random.nextDouble() * 45);
            case "ACCUMULATOR": return (random.nextDouble() * 40) - 20;
            default:            return random.nextDouble() * 10;
        }
    }

    public static void main(String[] args) {
        MeasurementService service = new MeasurementService();
        service.recoverState();
        if (args.length == 0) { printUsage(); return; }
        switch (args[0]) {
            case "report": if (args.length < 3) { printUsage(); return; }
                service.reportMeasurement(args[1], Double.parseDouble(args[2])); break;
            case "simulate":
                service.simulate(args.length >= 2 ? Integer.parseInt(args[1]) : 20); break;
            default: printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Uso:");
        System.out.println("  java MeasurementService report <nodeId> <valorKW>");
        System.out.println("  java MeasurementService simulate [numMediciones]");
    }
}