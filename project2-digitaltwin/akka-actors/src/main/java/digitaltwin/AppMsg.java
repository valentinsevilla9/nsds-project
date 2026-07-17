package digitaltwin;

import java.io.Serializable;

// mensaje entre actores que imita el trafico de aplicacion real
// (Serializable porque ahora puede viajar por red hasta un actor remoto)
public record AppMsg(String fromNodeId, int seqNum) implements Serializable {
}
