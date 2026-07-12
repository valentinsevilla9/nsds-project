package digitaltwin;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.util.LinkedList;

// El gemelo digital de un nodo IoT. Guarda el mismo estado que pide el
// enunciado (padre actual, historial de K padres, periodo T) y nada
// mas, solo reacciona a los mensajes que le manda Node-RED. 
// Toda la coordinacion vive en Node-RED, no aqui.
public class IoTNodeActor extends AbstractActor {

    private static final int K = 3; // cuantos padres antiguos guardamos

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
                .match(NodeRecoveredMsg.class, this::onNodeRecovered)
                .build();
    }

    // cambio de padre: guardamos el que teniamos en el historial (maximo
    // K) y nos quedamos con el nuevo
    private void onParentUpdate(ParentUpdateMsg msg) {
        System.out.println("NODE " + nodeId + ": parent changed " +
                currentParent + " -> " + msg.newParentId());

        if (currentParent != null) {
            parentHistory.addFirst(currentParent);
            if (parentHistory.size() > K)
                parentHistory.removeLast();
        }
        currentParent = msg.newParentId();
        printState();
    }

    // mensaje de aplicacion, imita el trafico UDP real hacia el root
    private void onAppMsg(AppMsg msg) {
        System.out.println("NODE " + nodeId + ": app message from " +
                msg.fromNodeId() + " seq=" + msg.seqNum());
    }

    // cambio de periodo. Aqui solo lo guardamos
    private void onSetPeriod(SetPeriodMsg msg) {
        System.out.println("NODE " + nodeId + ": period changed " +
                periodMs + "ms -> " + msg.periodMs() + "ms");
        this.periodMs = msg.periodMs();
    }

    private void onNodeCrash(NodeCrashMsg msg) {
        System.out.println("NODE " + nodeId + ": CRASHED — entering recovery mode");
        this.crashed = true;
    }

    // el nodo real ha vuelto a dar señales de vida
    private void onNodeRecovered(NodeRecoveredMsg msg) {
        System.out.println("NODE " + nodeId + ": RECOVERED — leaving recovery mode");
        this.crashed = false;
        printState();
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
