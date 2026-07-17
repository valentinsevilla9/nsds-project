package digitaltwin;

import java.io.Serializable;

// cambia el periodo T del actor. Node-RED es quien se encarga de propagar esto al nodo IoT real
public record SetPeriodMsg(int periodMs) implements Serializable {
}
