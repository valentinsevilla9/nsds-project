package smartgrid.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class PresentationService {

    private static final String SERVER_ADDR = "localhost:9092";
    private static final String NODE_EVENTS_TOPIC = "node-events";
    private static final String BILLING_TOPIC = "billing-records";

    private final Map<String, NodeView> nodes = new HashMap<>();
    private final Map<String, List<BillingView>> billingByNode = new HashMap<>();
    private final Gson gson = new Gson();

    public static class NodeView {
        public String nodeId, districtId, nodeType, ownerId;
        public NodeView(String nodeId, String districtId, String nodeType, String ownerId) {
            this.nodeId = nodeId; this.districtId = districtId;
            this.nodeType = nodeType; this.ownerId = ownerId;
        }
    }

    public static class BillingView {
        public String nodeId;
        public double totalEnergyKwh, cost;
        public int measurementCount;
        public long periodEnd;
        public BillingView(String nodeId, double totalEnergyKwh, double cost,
                           int measurementCount, long periodEnd) {
            this.nodeId = nodeId; this.totalEnergyKwh = totalEnergyKwh;
            this.cost = cost; this.measurementCount = measurementCount;
            this.periodEnd = periodEnd;
        }
    }

    // ── Fault Recovery con assign+seek ───────────────────────────────────────

    private void recoverState() {
        System.out.println("[PresentationService] Recuperando estado...");

        recoverFromTopic(NODE_EVENTS_TOPIC, record -> {
            Map event = gson.fromJson(record.value(), Map.class);
            String eventType = (String) event.get("type");
            String nodeId = (String) event.get("nodeId");
            switch (eventType) {
                case "NodeCreated": case "NodeUpdated":
                    nodes.put(nodeId, new NodeView(nodeId,
                            (String) event.get("districtId"),
                            (String) event.get("nodeType"),
                            (String) event.get("ownerId"))); break;
                case "NodeDeleted":
                    nodes.remove(nodeId);
                    billingByNode.remove(nodeId); break;
            }
        });

        recoverFromTopic(BILLING_TOPIC, record -> {
            Map event = gson.fromJson(record.value(), Map.class);
            String nodeId = (String) event.get("nodeId");
            double energy = ((Number) event.get("totalEnergyKwh")).doubleValue();
            double cost = ((Number) event.get("cost")).doubleValue();
            int count = ((Number) event.get("measurementCount")).intValue();
            long periodEnd = ((Number) event.get("periodEnd")).longValue();
            billingByNode.computeIfAbsent(nodeId, k -> new ArrayList<>())
                    .add(new BillingView(nodeId, energy, cost, count, periodEnd));
        });

        System.out.println("[PresentationService] Estado recuperado: "
                + nodes.size() + " nodos, "
                + billingByNode.values().stream().mapToInt(List::size).sum()
                + " registros de facturacion.");
    }

    private void recoverFromTopic(String topic,
            java.util.function.Consumer<ConsumerRecord<String, String>> handler) {

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ps-recovery-" + UUID.randomUUID());
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

    // ── Vistas ───────────────────────────────────────────────────────────────

    public void showUser(String userId) {
        List<NodeView> userNodes = new ArrayList<>();
        for (NodeView node : nodes.values())
            if (userId.equals(node.ownerId)) userNodes.add(node);

        if (userNodes.isEmpty()) {
            System.out.println("[PresentationService] Usuario '" + userId + "' no tiene nodos."); return;
        }

        System.out.println("==============================================");
        System.out.println("  Panel de usuario: " + userId);
        System.out.println("==============================================");

        double totalCost = 0.0;
        for (NodeView node : userNodes) {
            System.out.println("\n  Nodo: " + node.nodeId
                    + "  |  Tipo: " + node.nodeType
                    + "  |  Distrito: " + node.districtId);
            List<BillingView> records = billingByNode.getOrDefault(node.nodeId, Collections.emptyList());
            if (records.isEmpty()) {
                System.out.println("    (sin registros de facturacion aun)");
            } else {
                double nodeCost = 0, nodeEnergy = 0;
                System.out.println("    Historial:");
                for (int i = 0; i < records.size(); i++) {
                    BillingView br = records.get(i);
                    nodeCost += br.cost; nodeEnergy += br.totalEnergyKwh;
                    System.out.printf("    [%d] %.4f kWh -> %.4f EUR (%d mediciones)%n",
                            i+1, br.totalEnergyKwh, br.cost, br.measurementCount);
                }
                System.out.printf("    --- Total: %.4f kWh | %.4f EUR%n", nodeEnergy, nodeCost);
                totalCost += nodeCost;
            }
        }
        System.out.println("\n==============================================");
        System.out.printf("  TOTAL USUARIO: %.4f EUR%n", totalCost);
        System.out.println("==============================================");
    }

    public void showSummary() {
        Map<String, Integer> nodesByDistrict = new TreeMap<>();
        Map<String, Double> costByDistrict = new TreeMap<>();
        Map<String, Double> energyByDistrict = new TreeMap<>();

        for (NodeView node : nodes.values()) {
            nodesByDistrict.merge(node.districtId, 1, Integer::sum);
            for (BillingView br : billingByNode.getOrDefault(node.nodeId, Collections.emptyList())) {
                costByDistrict.merge(node.districtId, br.cost, Double::sum);
                energyByDistrict.merge(node.districtId, br.totalEnergyKwh, Double::sum);
            }
        }

        System.out.println("==============================================");
        System.out.println("  Resumen global por distrito");
        System.out.println("==============================================");
        if (nodesByDistrict.isEmpty()) {
            System.out.println("  No hay distritos registrados.");
        } else {
            for (String d : nodesByDistrict.keySet()) {
                System.out.println("\n  Distrito: " + d);
                System.out.println("    Nodos:   " + nodesByDistrict.get(d));
                System.out.printf("    Energia: %.4f kWh%n", energyByDistrict.getOrDefault(d, 0.0));
                System.out.printf("    Coste:   %.4f EUR%n", costByDistrict.getOrDefault(d, 0.0));
            }
        }
        System.out.println("==============================================");
    }

    public static void main(String[] args) {
        PresentationService service = new PresentationService();
        service.recoverState();
        if (args.length == 0) { printUsage(); return; }
        switch (args[0]) {
            case "show": if (args.length < 2) { printUsage(); return; }
                service.showUser(args[1]); break;
            case "summary": service.showSummary(); break;
            default: printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Uso:");
        System.out.println("  java PresentationService show <userId>");
        System.out.println("  java PresentationService summary");
    }
}