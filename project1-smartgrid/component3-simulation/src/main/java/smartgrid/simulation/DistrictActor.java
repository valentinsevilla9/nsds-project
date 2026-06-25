package smartgrid.simulation;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;



/**
 * DistrictActor — agrega las mediciones de todos los nodos de un distrito.
 *
 * Recibe EnergyMsg de cada NodeActor en su distrito y mantiene:
 * - balance total del distrito (suma de producers - consumers)
 * - estado de carga del acumulador (kWh)
 *
 * Cuando ha recibido mediciones de todos los nodos en el paso actual,
 * publica un DistrictBalanceMsg al SimulationMain.
 *
 * Patrón idéntico al SensorProcessorActor del profesor.
 */
public class DistrictActor extends AbstractActor {

    private final String districtId;
    private final int numNodes;
    private final ActorRef monitor; // recibe el resumen del distrito

    // Estado del distrito
    private double accumulatorCharge = 0.0; // kWh acumulados
    private int currentStep = 0;
    private double stepBalance = 0.0;
    private int receivedInStep = 0;

    public DistrictActor(String districtId, int numNodes, ActorRef monitor) {
        this.districtId = districtId;
        this.numNodes = numNodes;
        this.monitor = monitor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(EnergyMsg.class, this::onEnergy)
                .build();
    }

    private void onEnergy(EnergyMsg msg) {
        stepBalance += msg.getValue();
        receivedInStep++;

        // Cuando llegan todos los nodos del paso actual, publicar resumen
        if (receivedInStep >= numNodes) {
            // El acumulador absorbe o libera energía según el balance
            accumulatorCharge += stepBalance / 60.0; // convertir kW a kWh (paso ~1min)
            if (accumulatorCharge < 0)
                accumulatorCharge = 0; // no puede ser negativo

            System.out.println("DISTRICT " + districtId +
                    " [step=" + currentStep + "]:" +
                    " balance=" + String.format("%.2f", stepBalance) + " kW" +
                    " accumulator=" + String.format("%.4f", accumulatorCharge) + " kWh");

            monitor.tell(new DistrictBalanceMsg(districtId, stepBalance,
                    accumulatorCharge, currentStep), self());

            // Resetear para el siguiente paso
            currentStep++;
            stepBalance = 0.0;
            receivedInStep = 0;
        }
    }

    static Props props(String districtId, int numNodes, ActorRef monitor) {
        return Props.create(DistrictActor.class, districtId, numNodes, monitor);
    }
}