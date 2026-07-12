package smartgrid.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

// Servicio de usuarios: alta, baja y modificacion. No guarda nada en
// una base de datos, todo vive en el Map "users" que tenemos en memoria,
// y se reconstruye leyendo el topic "user-events" cada vez que arranca.
public class AccountService {

    private static final String SERVER_ADDR = "localhost:9092";
    private static final String USER_EVENTS_TOPIC = "user-events";

    private final Map<String, User> users = new HashMap<>();
    private final Gson gson = new Gson();

    // como guardamos un usuario en memoria
    public static class User {
        public String userId, name, email;

        public User(String userId, String name, String email) {
            this.userId = userId;
            this.name = name;
            this.email = email;
        }
    }

    // lo que mandamos a Kafka cada vez que pasa algo con un usuario
    public static class UserEvent {
        public String type, userId, name, email;
        public long timestamp;

        public UserEvent(String type, String userId, String name, String email) {
            this.type = type;
            this.userId = userId;
            this.name = name;
            this.email = email;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // Al arrancar nos leemos todo el historial de user-events desde el
    // principio y vamos aplicando cada evento como si acabara de pasar,
    // asi reconstruimos el Map de usuarios tal cual estaba antes de
    // caernos. Esto es el fault recovery que pide el enunciado.
    private void recoverState() {
        System.out.println("[AccountService] Recovering state from '" + USER_EVENTS_TOPIC + "'...");
        KafkaSupport.replayTopic(SERVER_ADDR, USER_EVENTS_TOPIC,
                record -> applyEvent(gson.fromJson(record.value(), UserEvent.class)));
        System.out.println("[AccountService] State recovered: " + users.size() + " users.");
    }

    // aplica un evento al estado en memoria - lo llamamos tanto al
    // recuperar el historial como justo despues de publicar uno nuevo
    private void applyEvent(UserEvent event) {
        switch (event.type) {
            case "UserRegistered":
            case "UserUpdated":
                users.put(event.userId, new User(event.userId, event.name, event.email));
                break;
            case "UserDeleted":
                users.remove(event.userId);
                break;
        }
    }

    private void publish(UserEvent event) {
        try (KafkaProducer<String, String> p = KafkaSupport.createProducer(SERVER_ADDR)) {
            p.send(new ProducerRecord<>(USER_EVENTS_TOPIC, event.userId, gson.toJson(event)),
                    (m, ex) -> {
                        if (ex != null)
                            System.err.println(ex.getMessage());
                        else
                            System.out.println("[AccountService] Event published -> offset=" + m.offset());
                    });
            p.flush();
        }
    }

    public void registerUser(String userId, String name, String email) {
        if (users.containsKey(userId)) {
            System.out.println("[AccountService] ERROR: User '" + userId + "' already exists.");
            return;
        }
        UserEvent event = new UserEvent("UserRegistered", userId, name, email);
        publish(event);
        applyEvent(event);
        System.out.println("[AccountService] User registered: " + userId);
    }

    public void updateUser(String userId, String name, String email) {
        if (!users.containsKey(userId)) {
            System.out.println("[AccountService] ERROR: User '" + userId + "' does not exist.");
            return;
        }
        UserEvent event = new UserEvent("UserUpdated", userId, name, email);
        publish(event);
        applyEvent(event);
        System.out.println("[AccountService] User updated: " + userId);
    }

    public void deleteUser(String userId) {
        if (!users.containsKey(userId)) {
            System.out.println("[AccountService] ERROR: User '" + userId + "' does not exist.");
            return;
        }
        UserEvent event = new UserEvent("UserDeleted", userId, null, null);
        publish(event);
        applyEvent(event);
        System.out.println("[AccountService] User deleted: " + userId);
    }

    public void listUsers() {
        if (users.isEmpty())
            System.out.println("[AccountService] No users.");
        else {
            System.out.println("[AccountService] Users (" + users.size() + "):");
            users.values().forEach(u -> System.out.println(
                    "  - " + u.userId + " | " + u.name + " | " + u.email));
        }
    }

    // el "frontend": lo hemos hecho como CLI porque el enunciado deja
    // elegir la tecnologia, y para probar esto a mano no hace falta nada mas
    public static void main(String[] args) {
        AccountService service = new AccountService();
        service.recoverState();
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "register":
                if (args.length < 4) {
                    printUsage();
                    return;
                }
                service.registerUser(args[1], args[2], args[3]);
                break;
            case "update":
                if (args.length < 4) {
                    printUsage();
                    return;
                }
                service.updateUser(args[1], args[2], args[3]);
                break;
            case "delete":
                if (args.length < 2) {
                    printUsage();
                    return;
                }
                service.deleteUser(args[1]);
                break;
            case "list":
                service.listUsers();
                break;
            default:
                printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java AccountService register <userId> <name> <email>");
        System.out.println("  java AccountService update   <userId> <name> <email>");
        System.out.println("  java AccountService delete   <userId>");
        System.out.println("  java AccountService list");
    }
}
