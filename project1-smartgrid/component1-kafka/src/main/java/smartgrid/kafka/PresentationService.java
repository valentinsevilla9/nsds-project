package smartgrid.kafka;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// Este es el unico de los 5 que no publica nada, solo lee. Se monta su propia vista leyendo 
// node-events y billing-records enteros, y con eso responde a las dos consultas que
// pide el enunciado: 
// 1) Ver los nodos de un usuario con sus costes
// 2) Un resumen por distrito.
public class PresentationService {

    private static final String SERVER_ADDR = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
    private static final String NODE_EVENTS_TOPIC = "node-events";
    private static final String BILLING_TOPIC = "billing-records";

    private final Map<String, NodeView> nodes = new HashMap<>();
    private final Map<String, List<BillingView>> billingByNode = new HashMap<>();
    private final Gson gson = new Gson();

    public static class NodeView {
        public String nodeId, districtId, nodeType, ownerId;

        public NodeView(String nodeId, String districtId, String nodeType, String ownerId) {
            this.nodeId = nodeId;
            this.districtId = districtId;
            this.nodeType = nodeType;
            this.ownerId = ownerId;
        }
    }

    public static class BillingView {
        public String nodeId;
        public double totalEnergyKwh, cost;
        public int measurementCount;
        public long periodEnd;

        public BillingView(String nodeId, double totalEnergyKwh, double cost,
                int measurementCount, long periodEnd) {
            this.nodeId = nodeId;
            this.totalEnergyKwh = totalEnergyKwh;
            this.cost = cost;
            this.measurementCount = measurementCount;
            this.periodEnd = periodEnd;
        }
    }

    private void recoverState() {
        System.out.println("[PresentationService] Recovering state...");

        KafkaSupport.replayTopic(SERVER_ADDR, NODE_EVENTS_TOPIC, record -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> event = gson.fromJson(record.value(), Map.class);
            String eventType = (String) event.get("type");
            String nodeId = (String) event.get("nodeId");
            switch (eventType) {
                case "NodeCreated":
                case "NodeUpdated":
                    nodes.put(nodeId, new NodeView(nodeId,
                            (String) event.get("districtId"),
                            (String) event.get("nodeType"),
                            (String) event.get("ownerId")));
                    break;
                case "NodeDeleted":
                    nodes.remove(nodeId);
                    billingByNode.remove(nodeId);
                    break;
            }
        });

        // aqui no reemplazamos nada, vamos ANADIENDO cada BillingRecord a
        // la lista del nodo - es literalmente el historial de facturacion
        KafkaSupport.replayTopic(SERVER_ADDR, BILLING_TOPIC, record -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> event = gson.fromJson(record.value(), Map.class);
            String nodeId = (String) event.get("nodeId");
            double energy = ((Number) event.get("totalEnergyKwh")).doubleValue();
            double cost = ((Number) event.get("cost")).doubleValue();
            int count = ((Number) event.get("measurementCount")).intValue();
            long periodEnd = ((Number) event.get("periodEnd")).longValue();
            billingByNode.computeIfAbsent(nodeId, k -> new ArrayList<>())
                    .add(new BillingView(nodeId, energy, cost, count, periodEnd));
        });

        System.out.println("[PresentationService] State recovered: "
                + nodes.size() + " nodes, "
                + billingByNode.values().stream().mapToInt(List::size).sum()
                + " billing records.");
    }

    // panel de un usuario: sus nodos, y por cada uno el historial de
    // facturacion con el total sumado al final
    public void showUser(String userId) {
        List<NodeView> userNodes = new ArrayList<>();
        for (NodeView node : nodes.values())
            if (userId.equals(node.ownerId))
                userNodes.add(node);

        if (userNodes.isEmpty()) {
            System.out.println("[PresentationService] User '" + userId + "' has no nodes.");
            return;
        }

        System.out.println("\nUser panel: " + userId);

        double totalCost = 0.0;
        for (NodeView node : userNodes) {
            System.out.println("\n  Node: " + node.nodeId
                    + "  |  Type: " + node.nodeType
                    + "  |  District: " + node.districtId);
            List<BillingView> records = billingByNode.getOrDefault(node.nodeId, Collections.emptyList());
            if (records.isEmpty()) {
                System.out.println("    (no billing records yet)");
            } else {
                double nodeCost = 0, nodeEnergy = 0;
                System.out.println("    History:");
                for (int i = 0; i < records.size(); i++) {
                    BillingView br = records.get(i);
                    nodeCost += br.cost;
                    nodeEnergy += br.totalEnergyKwh;
                    System.out.printf("    [%d] %.4f kWh -> %.4f EUR (%d measurements)%n",
                            i + 1, br.totalEnergyKwh, br.cost, br.measurementCount);
                }
                System.out.printf("    Total: %.4f kWh | %.4f EUR%n", nodeEnergy, nodeCost);
                totalCost += nodeCost;
            }
        }
        System.out.printf("%nUser total: %.4f EUR%n", totalCost);
    }

    // resumen global: cuantos nodos, energia y coste por cada distrito
    // (TreeMap para que salgan ordenados alfabeticamente sin currarnoslo)
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

        System.out.println("\nGlobal summary by district");
        if (nodesByDistrict.isEmpty()) {
            System.out.println("  No districts registered.");
        } else {
            for (String d : nodesByDistrict.keySet()) {
                System.out.println("\n  District: " + d);
                System.out.println("    Nodes:  " + nodesByDistrict.get(d));
                System.out.printf("    Energy: %.4f kWh%n", energyByDistrict.getOrDefault(d, 0.0));
                System.out.printf("    Cost:   %.4f EUR%n", costByDistrict.getOrDefault(d, 0.0));
            }
        }
    }

    public static void main(String[] args) {
        PresentationService service = new PresentationService();
        service.recoverState();
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "show":
                if (args.length < 2) {
                    printUsage();
                    return;
                }
                service.showUser(args[1]);
                break;
            case "summary":
                service.showSummary();
                break;
            default:
                printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java PresentationService show <userId>");
        System.out.println("  java PresentationService summary");
    }
}
