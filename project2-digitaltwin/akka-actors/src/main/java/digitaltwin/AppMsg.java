package digitaltwin;

/**
 * Mensaje de aplicación entre actores, mimicking el tráfico UDP
 * que los nodos IoT envían periódicamente al root RPL.
 * Requisito b) del enunciado: "Any flow of application messages should be
 * mimicked"
 */
public class AppMsg {
    private final String fromNodeId;
    private final int seqNum;

    public AppMsg(String fromNodeId, int seqNum) {
        this.fromNodeId = fromNodeId;
        this.seqNum = seqNum;
    }

    public String getFromNodeId() {
        return fromNodeId;
    }

    public int getSeqNum() {
        return seqNum;
    }
}