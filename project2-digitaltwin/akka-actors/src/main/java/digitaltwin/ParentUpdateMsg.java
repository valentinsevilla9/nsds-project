package digitaltwin;

/**
 * Mensaje enviado por Node-RED al actor cuando un nodo IoT cambia de padre en
 * el árbol RPL.
 * Requisito a) del enunciado: "Any change of parent in the tree should be
 * mirrored in the actor"
 */
public class ParentUpdateMsg {
    private final String nodeId;
    private final String newParentId;

    public ParentUpdateMsg(String nodeId, String newParentId) {
        this.nodeId = nodeId;
        this.newParentId = newParentId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNewParentId() {
        return newParentId;
    }
}
