package digitaltwin;

/**
 * Mensaje para cambiar el período de generación T de un actor.
 * Cuando Node-RED recibe este cambio, lo propaga al nodo IoT real.
 * Requisito c) del enunciado: "Changing T in an actor should be mirrored in the
 * IoT node"
 */
public class SetPeriodMsg {
    private final int periodMs;

    public SetPeriodMsg(int periodMs) {
        this.periodMs = periodMs;
    }

    public int getPeriodMs() {
        return periodMs;
    }
}