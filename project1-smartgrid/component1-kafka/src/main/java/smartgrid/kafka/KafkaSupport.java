package smartgrid.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;

// No es compartir estado entre servicios (eso sigue prohibido
// por el enunciado) - es solo codigo, sin datos ni conexiones vivas. Cada
// servicio lo usa por su cuenta, con su propio topic y su propio Map en
// memoria. Es lo mismo que compartir una libreria.
public class KafkaSupport {

    private KafkaSupport() {
    }

    // Lee un topic ENTERO desde el principio y le pasa cada mensaje al
    // "handler" que le pases. Es lo que usa cada servicio al arrancar para
    // reconstruir su estado si se ha caido (fault recovery). Usamos
    // assign()+seekToBeginning() en vez de subscribe() porque asi
    // controlamos nosotros el avance y sabemos exactamente cuando hemos
    // llegado al final del topic, sin depender de rebalances de grupo.
    public static void replayTopic(String server, String topic, Consumer<ConsumerRecord<String, String>> handler) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "recovery-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty())
                return;

            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : partitions)
                tps.add(new TopicPartition(pi.topic(), pi.partition()));

            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            if (endOffsets.values().stream().noneMatch(o -> o > 0))
                return; // topic vacio, nada que recuperar

            Map<TopicPartition, Long> current = new HashMap<>();
            tps.forEach(tp -> current.put(tp, 0L));

            while (!reachedEnd(current, endOffsets)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(3, ChronoUnit.SECONDS));
                for (ConsumerRecord<String, String> record : records) {
                    handler.accept(record);
                    current.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                }
            }
        }
    }

    private static boolean reachedEnd(Map<TopicPartition, Long> current, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet())
            if (current.getOrDefault(e.getKey(), 0L) < e.getValue())
                return false;
        return true;
    }

    // Producer con la config que usan los 4 servicios que publican algo:
    // acks=all para no perder nada y todo como String porque los eventos van serializados en JSON.
    public static KafkaProducer<String, String> createProducer(String server) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }
}
