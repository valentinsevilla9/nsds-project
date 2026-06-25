package smartgrid.simulation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * SimulationMain — simulación distribuida del Componente 3.
 *
 * Cada instancia de este proceso gestiona un subconjunto de distritos
 * y publica sus resultados al topic Kafka "simulation-results".
 * Esto permite demostrar distribución real entre múltiples máquinas:
 *
 * Máquina A: java SimulationMain A 0 2 localhost:9092
 * Máquina B: java SimulationMain A 2 4 IP_MAQUINA_A:9092
 *
 * Estrategias de distribución de actores:
 * A (by-district): todos los actores de un distrito en el mismo proceso
 * B (round-robin): actores de distintos distritos intercalados
 *
 * Uso:
 * java SimulationMain <strategy> <districtFrom> <districtTo> [kafkaBroker]
 * [simSteps]
 * strategy: A | B
 * districtFrom: primer distrito a gestionar (inclusive)
 * districtTo: último distrito a gestionar (exclusive)
 * kafkaBroker: host:port del broker Kafka (default: localhost:9092)
 * simSteps: pasos de simulación (default: 10)
 */
public class SimulationMain {

    private static final int NODES_PER_DISTRICT = 4;
    private static final String RESULTS_TOPIC = "simulation-results";

    public static void main(String[] args) throws InterruptedException {

        String strategy = args.length > 0 ? args[0] : "A";
        int districtFrom = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        int districtTo = args.length > 2 ? Integer.parseInt(args[2]) : 2;
        String kafkaBroker = args.length > 3 ? args[3] : "localhost:9092";
        int simSteps = args.length > 4 ? Integer.parseInt(args[4]) : 10;

        int numDistricts = districtTo - districtFrom;

        System.out.println("=== Smart Grid Distributed Simulation ===");
        System.out.println("Strategy:     " + strategy);
        System.out.println("Districts:    " + districtFrom + " to " + (districtTo - 1));
        System.out.println("Kafka:        " + kafkaBroker);
        System.out.println("Steps:        " + simSteps);
        System.out.println("=========================================\n");

        // Producer Kafka para publicar resultados
        final KafkaProducer<String, String> producer = createProducer(kafkaBroker);

        final ActorSystem sys = ActorSystem.create("SmartGridSimulation");

        // Monitor publica resultados a Kafka
        final ActorRef monitor = sys.actorOf(
                KafkaMonitorActor.props(producer, RESULTS_TOPIC), "monitor");

        // Crear actores de distrito
        List<ActorRef> districtActors = new ArrayList<>();
        for (int d = districtFrom; d < districtTo; d++) {
            String districtId = "district-" + d;
            ActorRef da = sys.actorOf(
                    DistrictActor.props(districtId, NODES_PER_DISTRICT, monitor),
                    "district-" + d);
            districtActors.add(da);
        }

        // Crear actores de nodo según estrategia
        List<List<ActorRef>> nodesByDistrict = new ArrayList<>();
        for (int d = 0; d < numDistricts; d++)
            nodesByDistrict.add(new ArrayList<>());

        if (strategy.equals("A")) {
            for (int d = 0; d < numDistricts; d++) {
                for (int n = 0; n < NODES_PER_DISTRICT; n++) {
                    String nodeId = "d" + (districtFrom + d) + "-node" + n;
                    ActorRef node = sys.actorOf(
                            NodeActor.props(nodeId, getNodeType(n), districtActors.get(d)),
                            nodeId);
                    nodesByDistrict.get(d).add(node);
                }
            }
        } else {
            for (int n = 0; n < NODES_PER_DISTRICT; n++) {
                for (int d = 0; d < numDistricts; d++) {
                    String nodeId = "d" + (districtFrom + d) + "-node" + n;
                    ActorRef node = sys.actorOf(
                            NodeActor.props(nodeId, getNodeType(n), districtActors.get(d)),
                            nodeId);
                    nodesByDistrict.get(d).add(node);
                }
            }
        }

        TimeUnit.SECONDS.sleep(1);

        long startTime = System.currentTimeMillis();
        for (int step = 0; step < simSteps; step++) {
            System.out.println("\n--- Step " + step + " ---");
            for (List<ActorRef> nodes : nodesByDistrict)
                for (ActorRef node : nodes)
                    node.tell(new SimulateMsg(), ActorRef.noSender());
            TimeUnit.MILLISECONDS.sleep(500);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("\n=== Completed: strategy=" + strategy +
                " districts=" + districtFrom + "-" + (districtTo - 1) +
                " time=" + elapsed + "ms ===");

        TimeUnit.SECONDS.sleep(2);
        producer.close();
        sys.terminate();
    }

    private static KafkaProducer<String, String> createProducer(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private static String getNodeType(int n) {
        if (n == NODES_PER_DISTRICT - 1)
            return "ACCUMULATOR";
        if (n < (int) (NODES_PER_DISTRICT * 0.6))
            return "PRODUCER";
        return "CONSUMER";
    }
}