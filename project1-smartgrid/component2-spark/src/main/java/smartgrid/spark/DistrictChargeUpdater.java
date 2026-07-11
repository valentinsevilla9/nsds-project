package smartgrid.spark;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;

import java.util.Iterator;

/**
 * Integra el balance energético de un distrito en su estado de carga
 * (kWh), igual que DistrictActor (Akka) y district_simulation.c (MPI):
 * chargeKwh += balance_kW * horasTranscurridas, clamped a >= 0. La
 * diferencia con esos dos es que aquí el "paso" es un micro-batch de
 * Spark, así que se usa el tiempo real entre eventos en vez de asumir un
 * intervalo fijo.
 */
public class DistrictChargeUpdater
        implements MapGroupsWithStateFunction<String, MeasurementBean, DistrictChargeState, DistrictChargeUpdate> {

    @Override
    public DistrictChargeUpdate call(String districtId, Iterator<MeasurementBean> values,
            GroupState<DistrictChargeState> state) {

        DistrictChargeState current = state.exists() ? state.get() : new DistrictChargeState();

        double batchBalanceKw = 0.0;
        long numMeasurements = 0;
        long maxEventTimeMillis = current.getLastEventTimeMillis();

        while (values.hasNext()) {
            MeasurementBean m = values.next();
            batchBalanceKw += m.getValue();
            numMeasurements++;
            if (m.getTimestamp() > maxEventTimeMillis) {
                maxEventTimeMillis = m.getTimestamp();
            }
        }

        if (current.getLastEventTimeMillis() > 0) {
            double elapsedHours = (maxEventTimeMillis - current.getLastEventTimeMillis()) / 3_600_000.0;
            if (elapsedHours > 0) {
                double newCharge = current.getAccumulatorChargeKwh() + batchBalanceKw * elapsedHours;
                current.setAccumulatorChargeKwh(Math.max(0.0, newCharge));
            }
        }
        current.setLastEventTimeMillis(maxEventTimeMillis);
        state.update(current);

        return new DistrictChargeUpdate(districtId, batchBalanceKw, numMeasurements,
                current.getAccumulatorChargeKwh());
    }
}
