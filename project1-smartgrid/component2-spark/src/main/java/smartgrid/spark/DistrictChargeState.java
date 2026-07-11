package smartgrid.spark;

import java.io.Serializable;

/**
 * Estado persistido por Spark entre micro-batches para cada distrito
 * (mapGroupsWithState de Query 1 en EnergyAnalytics): carga acumulada del
 * distrito (kWh, igual que accumulatorCharge en DistrictActor/MPI) y el
 * timestamp del último evento procesado, necesario para integrar potencia
 * (kW) a energía (kWh) usando el tiempo real transcurrido entre eventos
 * en vez de asumir un intervalo fijo.
 */
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
