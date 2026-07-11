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

public class BillingService {

    private static final String SERVER_ADDR = "localhost:9092";
    private static final String MEASUREMENTS_TOPIC = "measurements";
    private static final String BILLING_TOPIC = "billing-records";
    private static final String CONSUMER_GROUP = "billing-service-group";
    private static final int BILLING_WINDOW_SIZE = 5;
    private static final double CONSUMER_RATE = 0.25;
    private static final double PRODUCER_CREDIT_RATE = 0.10;

    private final Map<String, Double> accumulatedEnergy = new HashMap<>();
    private final Map<String, Integer> measurementCount = new HashMap<>();
    private final Map<String, String> nodeTypes = new HashMap<>();
    private final Map<String, String> nodeDistricts = new HashMap<>();
    private final Gson gson = new Gson();

    // Necesarios para integrar energia con el tiempo real transcurrido entre
    // mediciones (en vez de asumir 1 medicion = 1 minuto) y para no comitear
    // nunca el offset de una medicion cuya ventana de facturacion sigue abierta.
    private final Map<String, Long> lastMeasurementTimestamp = new HashMap<>();
    private final Map<String, Long> windowStartTimestamp = new HashMap<>();
    private final Map<String, Long> windowStartOffset = new HashMap<>();
    private final Map<String, TopicPartition> nodePartition = new HashMap<>();

    public static class BillingRecord {
        public String type = "UsageRecordCreated";
        public String nodeId, nodeType, districtId;
        public double totalEnergyKwh, cost;
        public int measurementCount;
        public long periodStart, periodEnd;
        public BillingRecord(String nodeId, String nodeType, String districtId,
                             double totalEnergyKwh, double cost, int count,
                             long periodStart, long periodEnd) {
            this.nodeId = nodeId; this.nodeType = nodeType; this.districtId = districtId;
            this.totalEnergyKwh = totalEnergyKwh; this.cost = cost;
            this.measurementCount = count; this.periodStart = periodStart; this.periodEnd = periodEnd;
        }
    }

    // ── Fault Recovery con assign+seek ───────────────────────────────────────

    private void recoverState() {
        System.out.println("[BillingService] Recuperando estado desde billing-records...");

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-recovery-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        int recoveredRecords = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(BILLING_TOPIC);
            if (partitions != null && !partitions.isEmpty()) {
                List<TopicPartition> tps = new ArrayList<>();
                for (PartitionInfo pi : partitions)
                    tps.add(new TopicPartition(pi.topic(), pi.partition()));

                consumer.assign(tps);
                consumer.seekToBeginning(tps);

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
                if (endOffsets.values().stream().anyMatch(o -> o > 0)) {
                    Map<TopicPartition, Long> current = new HashMap<>();
                    tps.forEach(tp -> current.put(tp, 0L));

                    while (!reachedEnd(current, endOffsets)) {
                        ConsumerRecords<String, String> records =
                                consumer.poll(Duration.of(3, ChronoUnit.SECONDS));
                        for (ConsumerRecord<String, String> record : records) {
                            BillingRecord br = gson.fromJson(record.value(), BillingRecord.class);
                            nodeTypes.put(br.nodeId, br.nodeType);
                            nodeDistricts.put(br.nodeId, br.districtId);
                            recoveredRecords++;
                            current.put(new TopicPartition(record.topic(), record.partition()),
                                    record.offset() + 1);
                        }
                    }
                }
            }
        }

        System.out.println("[BillingService] Estado recuperado: " + recoveredRecords + " registros previos.");
    }

    private boolean reachedEnd(Map<TopicPartition, Long> current, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet())
            if (current.getOrDefault(e.getKey(), 0L) < e.getValue()) return false;
        return true;
    }

    // ── Procesamiento en tiempo real (subscribe) ──────────────────────────────
    // El procesamiento continuo usa subscribe con consumer group fijo,
    // igual que en los ejemplos del profesor.

    private KafkaProducer<String, String> createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    public void run() {
        System.out.println("[BillingService] Iniciando consumo de mediciones...");

        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = createProducer()) {

            consumer.subscribe(Collections.singletonList(MEASUREMENTS_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (ConsumerRecord<String, String> record : records) {
                    processMeasurement(record, producer);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync(safeCommitOffsets(records));
                }
            }
        }
    }

    // Nunca comitea el offset de una medicion cuya ventana de facturacion
    // sigue abierta (measurementCount > 0 sin haber emitido BillingRecord):
    // si el proceso cae, Kafka la reentrega tal cual al reiniciar y el
    // estado parcial se reconstruye solo, sin necesidad de recuperarlo a mano.
    private Map<TopicPartition, OffsetAndMetadata> safeCommitOffsets(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> minPendingOffsetByPartition = new HashMap<>();
        for (Map.Entry<String, Long> e : windowStartOffset.entrySet()) {
            TopicPartition tp = nodePartition.get(e.getKey());
            if (tp != null) minPendingOffsetByPartition.merge(tp, e.getValue(), Math::min);
        }

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> partRecords = records.records(tp);
            long lastOffsetInBatch = partRecords.get(partRecords.size() - 1).offset() + 1;
            Long minPending = minPendingOffsetByPartition.get(tp);
            long safeOffset = (minPending != null) ? Math.min(minPending, lastOffsetInBatch) : lastOffsetInBatch;
            offsets.put(tp, new OffsetAndMetadata(safeOffset));
        }
        return offsets;
    }

    private void processMeasurement(ConsumerRecord<String, String> record, KafkaProducer<String, String> producer) {
        @SuppressWarnings("unchecked")
        Map<String, Object> event = gson.fromJson(record.value(), Map.class);
        String nodeId = (String) event.get("nodeId");
        String nodeType = (String) event.get("nodeType");
        String districtId = (String) event.get("districtId");
        double value = ((Number) event.get("value")).doubleValue();
        long timestamp = ((Number) event.get("timestamp")).longValue();

        nodeTypes.put(nodeId, nodeType);
        nodeDistricts.put(nodeId, districtId);
        nodePartition.put(nodeId, new TopicPartition(record.topic(), record.partition()));

        Long lastTs = lastMeasurementTimestamp.get(nodeId);
        if (lastTs != null && timestamp > lastTs) {
            double elapsedHours = (timestamp - lastTs) / 3_600_000.0;
            accumulatedEnergy.merge(nodeId, Math.abs(value) * elapsedHours, Double::sum);
        }
        lastMeasurementTimestamp.put(nodeId, timestamp);

        int count = measurementCount.merge(nodeId, 1, Integer::sum);
        if (count == 1) {
            windowStartTimestamp.put(nodeId, timestamp);
            windowStartOffset.put(nodeId, record.offset());
        }

        System.out.println("[BillingService] Medicion procesada: nodo=" + nodeId
                + " valor=" + String.format("%.2f", value) + " kW"
                + " acumulado=" + String.format("%.4f", accumulatedEnergy.getOrDefault(nodeId, 0.0)) + " kWh"
                + " [" + count + "/" + BILLING_WINDOW_SIZE + "]");

        if (count >= BILLING_WINDOW_SIZE)
            emitBillingRecord(nodeId, nodeType, districtId, producer, timestamp);
    }

    private void emitBillingRecord(String nodeId, String nodeType, String districtId,
                                   KafkaProducer<String, String> producer, long periodEnd) {
        double totalEnergy = accumulatedEnergy.getOrDefault(nodeId, 0.0);
        double cost = calculateCost(nodeType, totalEnergy);
        int count = measurementCount.getOrDefault(nodeId, 0);
        long periodStart = windowStartTimestamp.getOrDefault(nodeId, periodEnd);

        BillingRecord record = new BillingRecord(nodeId, nodeType, districtId,
                totalEnergy, cost, count, periodStart, periodEnd);

        producer.send(new ProducerRecord<>(BILLING_TOPIC, nodeId, gson.toJson(record)), (m, ex) -> {
            if (ex != null) System.err.println("[BillingService] Error: " + ex.getMessage());
            else System.out.println("[BillingService] * Billing record emitido -> nodo=" + nodeId
                    + " energia=" + String.format("%.4f", totalEnergy) + " kWh"
                    + " coste=" + String.format("%.4f", cost) + " EUR"
                    + " offset=" + m.offset());
        });
        producer.flush();

        accumulatedEnergy.put(nodeId, 0.0);
        measurementCount.put(nodeId, 0);
        windowStartTimestamp.remove(nodeId);
        windowStartOffset.remove(nodeId);
    }

    private double calculateCost(String nodeType, double energyKwh) {
        switch (nodeType) {
            case "CONSUMER":    return energyKwh * CONSUMER_RATE;
            case "PRODUCER":    return -energyKwh * PRODUCER_CREDIT_RATE;
            case "ACCUMULATOR": return 0.0;
            default:            return 0.0;
        }
    }

    public static void main(String[] args) {
        BillingService service = new BillingService();
        service.recoverState();
        service.run();
    }
}