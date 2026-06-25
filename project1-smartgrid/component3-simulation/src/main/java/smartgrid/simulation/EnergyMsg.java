package smartgrid.simulation;

import akka.actor.ActorRef;

/**
 * Mensaje que transporta una medición de energía entre actores.
 * Enviado por ProducerActor/ConsumerActor al DistrictActor correspondiente.
 */
public class EnergyMsg {
    private final String nodeId;
    private final String nodeType; // PRODUCER | CONSUMER | ACCUMULATOR
    private final double value; // kW: positivo=producción, negativo=consumo
    private final ActorRef sender;

    public EnergyMsg(String nodeId, String nodeType, double value, ActorRef sender) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.value = value;
        this.sender = sender;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeType() {
        return nodeType;
    }

    public double getValue() {
        return value;
    }

    public ActorRef getSender() {
        return sender;
    }
}
