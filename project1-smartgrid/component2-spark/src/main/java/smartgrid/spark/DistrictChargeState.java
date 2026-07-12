package smartgrid.spark;

import java.io.Serializable;

// Esto es lo que Spark guarda de un distrito entre un micro-batch y el siguiente
public class DistrictChargeState implements Serializable {

    private double accumulatorChargeKwh = 0.0;
    private long lastEventTimeMillis = -1L;

    public DistrictChargeState() {
    }

    public double getAccumulatorChargeKwh() {
        return accumulatorChargeKwh;
    }

    public void setAccumulatorChargeKwh(double accumulatorChargeKwh) {
        this.accumulatorChargeKwh = accumulatorChargeKwh;
    }

    public long getLastEventTimeMillis() {
        return lastEventTimeMillis;
    }

    public void setLastEventTimeMillis(long lastEventTimeMillis) {
        this.lastEventTimeMillis = lastEventTimeMillis;
    }
}
