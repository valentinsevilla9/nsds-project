package digitaltwin;

/**
 * Mensaje enviado por Node-RED al actor cuando el nodo IoT crashea.
 * Requisito d) del enunciado: "A crash of an IoT node should be mirrored in the
 * actor network"
 */
public class NodeCrashMsg {
    private final String nodeId;

    public NodeCrashMsg(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }
}