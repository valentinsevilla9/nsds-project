package digitaltwin;

import akka.actor.ActorSystem;
import com.typesafe.config.ConfigFactory;

import java.util.Scanner;

// Segundo proceso del digital twin, pensado para correr en OTRO portatil
// distinto al de DigitalTwinMain. Solo crea actores y se queda esperando
// mensajes por red - no expone ningun servidor HTTP, Node-RED nunca le
// habla directamente a este proceso.
//
// DigitalTwinMain le manda mensajes a estos actores via Akka Remoting
// (ActorSelection), exactamente igual que si fueran locales - por eso
// aqui no hace falta ninguna logica nueva, solo crear los actores y
// dejarlos vivos.
//
// Uso: java -cp akka-actors-1.0-jar-with-dependencies.jar digitaltwin.RemoteNodeHost [fromNodeId] [toNodeId]
public class RemoteNodeHost {

    public static void main(String[] args) throws Exception {
        int fromNodeId = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        int toNodeId = args.length > 1 ? Integer.parseInt(args[1]) : 4;

        System.out.println("=== Digital Twin - Remote Node Host ===");
        System.out.println("Hosting actors for nodes " + fromNodeId + " to " + toNodeId);
        System.out.println("\n");

        ActorSystem system = ActorSystem.create("DigitalTwin", ConfigFactory.load());

        // el padre inicial de estos nodos siempre es "1" (el root), igual
        // que en DigitalTwinMain - ningun nodo remoto es el root
        for (int i = fromNodeId; i <= toNodeId; i++) {
            String nodeId = String.valueOf(i);
            system.actorOf(IoTNodeActor.props(nodeId, "1", 4000), "node-" + nodeId);
            System.out.println("Created REMOTE actor for node " + nodeId + " (parent=1, period=4000ms)");
        }

        System.out.println("\nRemote actors ready, waiting for messages from DigitalTwinMain.");
        System.out.println("Press Enter to stop...");
        try (Scanner scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

        system.terminate();
    }
}
