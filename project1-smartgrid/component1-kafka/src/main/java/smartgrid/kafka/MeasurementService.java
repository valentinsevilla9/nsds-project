package smartgrid.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

// Recibe mediciones de energia de los nodos y las publica en el topic measurements. 
// Para saber a que distrito/tipo pertenece cada nodo (y no tener que pedirselo al usuario cada vez) nos copiamos esa info
// de node-events, igual que hace DistrictNodeManager con user-events.
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
            this.nodeId = nodeId;
            this.districtId = districtId;
            this.nodeType = nodeType;
        }
    }

    public static class MeasurementEvent {
        public String type = "MeasurementReported";
        public String nodeId, nodeType, districtId;
        public double value;
        public long timestamp;

        public MeasurementEvent(String nodeId, String nodeType, String districtId, double value) {
            this.nodeId = nodeId;
            this.nodeType = nodeType;
            this.districtId = districtId;
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // nos leemos node-events entero para saber que nodos existen antes
    // de aceptar ninguna medicion
    private void recoverState() {
        System.out.println("[MeasurementService] Recovering nodes from '" + NODE_EVENTS_TOPIC + "'...");
        KafkaSupport.replayTopic(SERVER_ADDR, NODE_EVENTS_TOPIC, record -> applyNodeEvent(record.value()));
        System.out.println("[MeasurementService] Nodes recovered: " + nodes.size());
    }

    private void applyNodeEvent(String json) {
        @SuppressWarnings("unchecked")
        Map<String, Object> event = gson.fromJson(json, Map.class);
        String eventType = (String) event.get("type");
        String nodeId = (String) event.get("nodeId");
        switch (eventType) {
            case "NodeCreated":
            case "NodeUpdated":
                nodes.put(nodeId, new NodeInfo(nodeId,
                        (String) event.get("districtId"),
                        (String) event.get("nodeType")));
                break;
            case "NodeDeleted":
                nodes.remove(nodeId);
                break;
        }
    }

    // medicion suelta, para probar a mano
    public void reportMeasurement(String nodeId, double value) {
        if (!nodes.containsKey(nodeId)) {
            System.out.println("[MeasurementService] ERROR: Node '" + nodeId + "' does not exist.");
            return;
        }
        NodeInfo node = nodes.get(nodeId);
        MeasurementEvent event = new MeasurementEvent(nodeId, node.nodeType, node.districtId, value);
        try (KafkaProducer<String, String> p = KafkaSupport.createProducer(SERVER_ADDR)) {
            p.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, node.districtId, gson.toJson(event)),
                    (m, ex) -> {
                        if (ex != null)
                            System.err.println(ex.getMessage());
                        else
                            System.out.println("[MeasurementService] Measurement published -> node="
                                    + nodeId + " value=" + value + " kW offset=" + m.offset());
                    });
            p.flush();
        }
    }

    // genera trafico de mentira sobre los nodos que ya existen, para no
    // tener que ir haciendo reportMeasurement uno a uno mientras probamos
    public void simulate(int numMeasurements) {
        if (nodes.isEmpty()) {
            System.out.println("[MeasurementService] No nodes. Create nodes first.");
            return;
        }
        List<NodeInfo> nodeList = new ArrayList<>(nodes.values());
        System.out.println("[MeasurementService] Simulating " + numMeasurements + " measurements...");
        try (KafkaProducer<String, String> p = KafkaSupport.createProducer(SERVER_ADDR)) {
            for (int i = 0; i < numMeasurements; i++) {
                NodeInfo node = nodeList.get(random.nextInt(nodeList.size()));
                double value = generateValue(node.nodeType);
                MeasurementEvent event = new MeasurementEvent(node.nodeId, node.nodeType, node.districtId, value);
                p.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, node.districtId, gson.toJson(event)));
                System.out.printf("  [%d/%d] %s (%s) -> %.2f kW%n",
                        i + 1, numMeasurements, node.nodeId, node.nodeType, value);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                }
            }
            p.flush();
        }
        System.out.println("[MeasurementService] Simulation completed.");
    }

    // rango de valores segun el tipo: producer siempre en positivo,
    // consumer siempre en negativo, accumulator puede ir para los dos lados
    private double generateValue(String nodeType) {
        switch (nodeType) {
            case "PRODUCER":
                return 10 + random.nextDouble() * 90;
            case "CONSUMER":
                return -(5 + random.nextDouble() * 45);
            case "ACCUMULATOR":
                return (random.nextDouble() * 40) - 20;
            default:
                return random.nextDouble() * 10;
        }
    }

    public static void main(String[] args) {
        MeasurementService service = new MeasurementService();
        service.recoverState();
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "report":
                if (args.length < 3) {
                    printUsage();
                    return;
                }
                service.reportMeasurement(args[1], Double.parseDouble(args[2]));
                break;
            case "simulate":
                service.simulate(args.length >= 2 ? Integer.parseInt(args[1]) : 20);
                break;
            default:
                printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java MeasurementService report <nodeId> <valueKW>");
        System.out.println("  java MeasurementService simulate [numMeasurements]");
    }
}
