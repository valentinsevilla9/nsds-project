package smartgrid.simulation;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.concurrent.ThreadLocalRandom;

/**
 * NodeActor — representa un nodo de la grid (producer, consumer o accumulator).
 *
 * Al recibir SimulateMsg, genera un valor de energía según distribución
 * estadística y lo envía al DistrictActor de su distrito.
 *
 * Patrón idéntico al TemperatureSensorActor del profesor:
 * - estado mínimo (tipo, distrito, referencia al actor de distrito)
 * - receiveBuilder().match() para cada tipo de mensaje
 * - Props.create() como factory method
 */
public class NodeActor extends AbstractActor {

    private final String nodeId;
    private final String nodeType;
    private final ActorRef districtActor;

    // Rangos de generación según tipo (igual que MeasurementService)
    private static final double PRODUCER_MIN = 10.0;
    private static final double PRODUCER_MAX = 100.0;
    private static final double CONSUMER_MIN = 5.0;
    private static final double CONSUMER_MAX = 50.0;
    private static final double ACCUM_RANGE = 20.0;

    public NodeActor(String nodeId, String nodeType, ActorRef districtActor) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.districtActor = districtActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SimulateMsg.class, this::onSimulate)
                .build();
    }

    private void onSimulate(SimulateMsg msg) {
        double value = generateValue();
        System.out.println("NODE " + nodeId + " (" + nodeType + "): generated " +
                String.format("%.2f", value) + " kW");
        districtActor.tell(new EnergyMsg(nodeId, nodeType, value, self()), self());
    }

    private double generateValue() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        switch (nodeType) {
            case "PRODUCER":
                return PRODUCER_MIN + rnd.nextDouble() * (PRODUCER_MAX - PRODUCER_MIN);
            case "CONSUMER":
                return -(CONSUMER_MIN + rnd.nextDouble() * (CONSUMER_MAX - CONSUMER_MIN));
            case "ACCUMULATOR":
                return (rnd.nextDouble() * 2 - 1) * ACCUM_RANGE;
            default:
                return 0.0;
        }
    }

    static Props props(String nodeId, String nodeType, ActorRef districtActor) {
        return Props.create(NodeActor.class, nodeId, nodeType, districtActor);
    }
}
