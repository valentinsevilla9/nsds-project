package smartgrid.simulation;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * SimulationConsumer — consume y muestra los resultados globales de la
 * simulación.
 *
 * Corre en cualquier máquina y muestra en tiempo real los balances
 * de todos los distritos publicados por los distintos procesos SimulationMain.
 *
 * Uso: java SimulationConsumer [kafkaBroker]
 */
public class SimulationConsumer {

    public static void main(String[] args) {
        String kafkaBroker = args.length > 0 ? args[0] : "localhost:9092";

        System.out.println("=== Simulation Global Monitor ===");
        System.out.println("Kafka: " + kafkaBroker);
        System.out.println("Listening on topic: simulation-results");
        System.out.println("=================================\n");

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simulation-monitor-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("simulation-results"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("[" + record.key() + "] " + record.value());
                }
            }
        }
    }
}