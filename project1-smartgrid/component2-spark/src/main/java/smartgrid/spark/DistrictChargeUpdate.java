package smartgrid.spark;

import java.io.Serializable;

/**
 * Salida de Query 1 (mapGroupsWithState) por cada distrito y micro-batch.
 */
public class DistrictChargeUpdate implements Serializable {

    private String districtId;
    private double batchBalanceKw;
    private long numMeasurements;
    private double accumulatorChargeKwh;

    public DistrictChargeUpdate() {
    }

    public DistrictChargeUpdate(String districtId, double batchBalanceKw,
            long numMeasurements, double accumulatorChargeKwh) {
        this.districtId = districtId;
        this.batchBalanceKw = batchBalanceKw;
        this.numMeasurements = numMeasurements;
        this.accumulatorChargeKwh = accumulatorChargeKwh;
    }

    public String getDistrictId() {
        return districtId;
    }

    public void setDistrictId(String districtId) {
        this.districtId = districtId;
    }

    public double getBatchBalanceKw() {
        return batchBalanceKw;
    }

    public void setBatchBalanceKw(double batchBalanceKw) {
        this.batchBalanceKw = batchBalanceKw;
    }

    public long getNumMeasurements() {
        return numMeasurements;
    }

    public void setNumMeasurements(long numMeasurements) {
        this.numMeasurements = numMeasurements;
    }

    public double getAccumulatorChargeKwh() {
        return accumulatorChargeKwh;
    }

    public void setAccumulatorChargeKwh(double accumulatorChargeKwh) {
        this.accumulatorChargeKwh = accumulatorChargeKwh;
    }

    @Override
    public String toString() {
        return "district=" + districtId
                + " batchBalance=" + String.format("%.2f", batchBalanceKw) + "kW"
                + " numMeasurements=" + numMeasurements
                + " accumulatorCharge=" + String.format("%.4f", accumulatorChargeKwh) + "kWh";
    }
}
