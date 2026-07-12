package smartgrid.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

// Se lee las mediciones de "measurements" y cada 5 mediciones de un mismo nodo, calcula cuanta 
// energia ha consumido/producido y publica un BillingRecord con el coste a "billing-records". 
// Es el servicio con más complejidad de fault-recovery de los 5, porque tiene un estado a
// medias (la ventana de facturacion en curso) que no vive en ningún topic.
public class BillingService {

    private static final String SERVER_ADDR = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
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

    // Para calcular la energia (kWh = kW * horas) necesitamos saber cuanto tiempo
    // ha pasado entre una medicion y la siguiente del mismo nodo, no vale asumir
    // que siempre pasa lo mismo. Y windowStartOffset es la clave de todo el fault
    // recovery: guardamos en que offset empezó la ventana que aún no hemos
    // facturado, para no comitear nunca más allá de ahí.
    private final Map<String, Long> lastMeasurementTimestamp = new HashMap<>();
    private final Map<String, Long> windowStartTimestamp = new HashMap<>();
    private final Map<String, Long> windowStartOffset = new HashMap<>();
    private final Map<String, TopicPartition> nodePartition = new HashMap<>();

    // lo que publicamos cada vez que se cierra una ventana de facturacion
    public static class BillingRecord {
        public String type = "UsageRecordCreated";
        public String nodeId, nodeType, districtId;
        public double totalEnergyKwh, cost;
        public int measurementCount;
        public long periodStart, periodEnd;

        public BillingRecord(String nodeId, String nodeType, String districtId,
                double totalEnergyKwh, double cost, int count,
                long periodStart, long periodEnd) {
            this.nodeId = nodeId;
            this.nodeType = nodeType;
            this.districtId = districtId;
            this.totalEnergyKwh = totalEnergyKwh;
            this.cost = cost;
            this.measurementCount = count;
            this.periodStart = periodStart;
            this.periodEnd = periodEnd;
        }
    }

    // Aqui solo recuperamos qué tipo/distrito tiene cada nodo, no la energia
    // acumulada a medias - esa se reconstruye sola gracias a safeCommitOffsets, no
    // hace falta guardarla en ningun sitio.
    private void recoverState() {
        System.out.println("[BillingService] Recovering state from billing-records...");
        int[] recoveredRecords = { 0 };
        KafkaSupport.replayTopic(SERVER_ADDR, BILLING_TOPIC, record -> {
            BillingRecord br = gson.fromJson(record.value(), BillingRecord.class);
            nodeTypes.put(br.nodeId, br.nodeType);
            nodeDistricts.put(br.nodeId, br.districtId);
            recoveredRecords[0]++;
        });
        System.out.println("[BillingService] State recovered: " + recoveredRecords[0] + " previous records.");
    }

    public void run() {
        System.out.println("[BillingService] Starting to consume measurements...");

        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
                KafkaProducer<String, String> producer = KafkaSupport.createProducer(SERVER_ADDR)) {

            consumer.subscribe(Collections.singletonList(MEASUREMENTS_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (ConsumerRecord<String, String> record : records) {
                    processMeasurement(record, producer);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync(safeCommitOffsets(records));
                }
            }
        }
    }

    // Esta es la clave del fault recovery de este servicio: Nunca comiteamos el
    // offset de una medicion cuya ventana de facturacion sigue abierta (le faltan
    // mediciones para llegar a las 5).
    // Asi, si el proceso se cae a mitad de una ventana, Kafka nos reentrega esas
    // mediciones tal cual al volver a arrancar, y el contador se reconstruye solo
    // con reprocesarlas. No hay que guardar nada aparte.
    private Map<TopicPartition, OffsetAndMetadata> safeCommitOffsets(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> minPendingOffsetByPartition = new HashMap<>();
        for (Map.Entry<String, Long> e : windowStartOffset.entrySet()) {
            TopicPartition tp = nodePartition.get(e.getKey());
            if (tp != null)
                minPendingOffsetByPartition.merge(tp, e.getValue(), Math::min);
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

        // kWh = kW * horas transcurridas desde la ultima medicion de este nodo.
        // La primera medicion de un nodo no suma nada porque notenemos con que comparar
        // todavia (hace falta al menos 2 puntos para saber cuanto tiempo ha pasado).
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

        System.out.println("[BillingService] Measurement processed: node=" + nodeId
                + " value=" + String.format("%.2f", value) + " kW"
                + " accumulated=" + String.format("%.4f", accumulatedEnergy.getOrDefault(nodeId, 0.0)) + " kWh"
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
            if (ex != null)
                System.err.println("[BillingService] Error: " + ex.getMessage());
            else
                System.out.println("[BillingService] * Billing record emitted -> node=" + nodeId
                        + " energy=" + String.format("%.4f", totalEnergy) + " kWh"
                        + " cost=" + String.format("%.4f", cost) + " EUR"
                        + " offset=" + m.offset());
        });
        producer.flush();

        // se cierra la ventana: a cero y a esperar las siguientes 5
        accumulatedEnergy.put(nodeId, 0.0);
        measurementCount.put(nodeId, 0);
        windowStartTimestamp.remove(nodeId);
        windowStartOffset.remove(nodeId);
    }

    // tarifa distinta segun el tipo de nodo: al que consume se le cobra,
    // al que produce se le abona (coste negativo), el acumulador ni cobra ni paga
    private double calculateCost(String nodeType, double energyKwh) {
        switch (nodeType) {
            case "CONSUMER":
                return energyKwh * CONSUMER_RATE;
            case "PRODUCER":
                return -energyKwh * PRODUCER_CREDIT_RATE;
            case "ACCUMULATOR":
                return 0.0;
            default:
                return 0.0;
        }
    }

    public static void main(String[] args) {
        BillingService service = new BillingService();
        service.recoverState();
        service.run();
    }
}
