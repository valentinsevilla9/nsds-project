package digitaltwin;

/**
 * Mensaje enviado por Node-RED al actor cuando el nodo IoT vuelve a dar
 * señales de vida tras un crash (heartbeat recuperado en el serial-bridge).
 * Parte de la recovery action del requisito d) del enunciado: "proper
 * recovery actions should be taken within the actor network".
 */
public class NodeRecoveredMsg {
    private final String nodeId;

    public NodeRecoveredMsg(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }
}
