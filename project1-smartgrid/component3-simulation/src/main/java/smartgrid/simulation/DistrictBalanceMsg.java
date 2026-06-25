package smartgrid.simulation;

/**
 * Mensaje publicado por DistrictActor al final de cada paso de simulación
 * con el balance energético agregado del distrito.
 */
public class DistrictBalanceMsg {
    private final String districtId;
    private final double totalBalance; // kW total del distrito
    private final double accumulatorCharge; // kWh del acumulador
    private final int step;

    public DistrictBalanceMsg(String districtId, double totalBalance,
            double accumulatorCharge, int step) {
        this.districtId = districtId;
        this.totalBalance = totalBalance;
        this.accumulatorCharge = accumulatorCharge;
        this.step = step;
    }

    public String getDistrictId() {
        return districtId;
    }

    public double getTotalBalance() {
        return totalBalance;
    }

    public double getAccumulatorCharge() {
        return accumulatorCharge;
    }

    public int getStep() {
        return step;
    }
}