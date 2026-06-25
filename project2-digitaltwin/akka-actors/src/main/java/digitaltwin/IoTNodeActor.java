package digitaltwin;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.LinkedList;

/**
 * IoTNodeActor — réplica virtual (Digital Twin) de un nodo IoT Contiki-NG.
 *
 * Estado del actor (mapeado 1:1 con el estado del nodo IoT real):
 * - currentParent: padre actual en el árbol RPL
 * - parentHistory: últimos K padres anteriores
 * - periodMs: período de generación de mensajes T (ms)
 * - crashed: si el nodo IoT correspondiente ha crasheado
 *
 * IMPORTANTE según el enunciado:
 * "Actors must not include any control functionality that does not map
 * to a concept of the corresponding IoT node"
 *
 * Por eso este actor NO implementa ninguna lógica de coordinación —
 * solo mantiene estado y reacciona a mensajes. Toda la lógica de
 * coordinación vive en Node-RED.
 *
 * Patrón idéntico al TemperatureSensorActor del profesor.
 */
public class IoTNodeActor extends AbstractActor {

    private static final int K = 3; // número de padres históricos a mantener

    private final String nodeId;
    private String currentParent;
    private final LinkedList<String> parentHistory = new LinkedList<>();
    private int periodMs;
    private boolean crashed = false;

    public IoTNodeActor(String nodeId, String initialParent, int initialPeriodMs) {
        this.nodeId = nodeId;
        this.currentParent = initialParent;
        this.periodMs = initialPeriodMs;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ParentUpdateMsg.class, this::onParentUpdate)
                .match(AppMsg.class, this::onAppMsg)
                .match(SetPeriodMsg.class, this::onSetPeriod)
                .match(NodeCrashMsg.class, this::onNodeCrash)
                .build();
    }

    /**
     * Requisito a): cambio de padre en el árbol RPL.
     * Actualiza el padre actual y mantiene el historial de los últimos K padres.
     */
    private void onParentUpdate(ParentUpdateMsg msg) {
        System.out.println("NODE " + nodeId + ": parent changed " +
                currentParent + " -> " + msg.getNewParentId());

        // Guardar padre anterior en historial (máximo K)
        if (currentParent != null) {
            parentHistory.addFirst(currentParent);
            if (parentHistory.size() > K)
                parentHistory.removeLast();
        }
        currentParent = msg.getNewParentId();
        printState();
    }

    /**
     * Requisito b): mensaje de aplicación recibido (mirroring del tráfico UDP).
     */
    private void onAppMsg(AppMsg msg) {
        System.out.println("NODE " + nodeId + ": app message from " +
                msg.getFromNodeId() + " seq=" + msg.getSeqNum());
    }

    /**
     * Requisito c): cambio de período T.
     * Node-RED propaga este cambio al nodo IoT real via MQTT.
     */
    private void onSetPeriod(SetPeriodMsg msg) {
        System.out.println("NODE " + nodeId + ": period changed " +
                periodMs + "ms -> " + msg.getPeriodMs() + "ms");
        this.periodMs = msg.getPeriodMs();
        // Node-RED se encarga de propagar este cambio al nodo IoT real
    }

    /**
     * Requisito d): crash del nodo IoT.
     * El actor se marca como crashed. La recuperación la coordina Node-RED.
     */
    private void onNodeCrash(NodeCrashMsg msg) {
        System.out.println("NODE " + nodeId + ": CRASHED — entering recovery mode");
        this.crashed = true;
        // Node-RED detectará este estado y tomará acciones de recovery
        // en el lado Contiki-NG (reiniciar el nodo en Cooja)
    }

    private void printState() {
        System.out.println("  State: parent=" + currentParent +
                " history=" + parentHistory +
                " period=" + periodMs + "ms" +
                " crashed=" + crashed);
    }

    static Props props(String nodeId, String initialParent, int initialPeriodMs) {
        return Props.create(IoTNodeActor.class, nodeId, initialParent, initialPeriodMs);
    }
}
