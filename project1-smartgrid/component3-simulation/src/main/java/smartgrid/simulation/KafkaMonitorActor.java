package smartgrid.simulation;

import akka.actor.AbstractActor;
import akka.actor.Props;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * KafkaMonitorActor — publica los resultados de cada distrito a Kafka.
 *
 * Sustituye al MonitorActor local. En lugar de imprimir por consola,
 * serializa cada DistrictBalanceMsg como JSON y lo publica al topic
 * "simulation-results" con districtId como key.
 *
 * Cualquier proceso en cualquier máquina puede consumir este topic
 * para ver el estado global de la simulación distribuida.
 */
public class KafkaMonitorActor extends AbstractActor {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaMonitorActor(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DistrictBalanceMsg.class, this::onBalance)
                .build();
    }

    private void onBalance(DistrictBalanceMsg msg) {
        String json = String.format(
                "{\"districtId\":\"%s\",\"step\":%d,\"balance\":%.4f,\"accumulator\":%.4f}",
                msg.getDistrictId(), msg.getStep(),
                msg.getTotalBalance(), msg.getAccumulatorCharge());

        producer.send(new ProducerRecord<>(topic, msg.getDistrictId(), json));

        System.out.println("MONITOR -> Kafka: " + json);
    }

    static Props props(KafkaProducer<String, String> producer, String topic) {
        return Props.create(KafkaMonitorActor.class, producer, topic);
    }
}