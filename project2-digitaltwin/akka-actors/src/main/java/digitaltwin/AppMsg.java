package digitaltwin;

// mensaje entre actores que imita el trafico de aplicacion real
public record AppMsg(String fromNodeId, int seqNum) {
}
