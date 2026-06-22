package smartgrid.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class AccountService {

    private static final String SERVER_ADDR = "localhost:9092";
    private static final String USER_EVENTS_TOPIC = "user-events";

    private final Map<String, User> users = new HashMap<>();
    private final Gson gson = new Gson();

    public static class User {
        public String userId, name, email;
        public User(String userId, String name, String email) {
            this.userId = userId; this.name = name; this.email = email;
        }
    }

    public static class UserEvent {
        public String type, userId, name, email;
        public long timestamp;
        public UserEvent(String type, String userId, String name, String email) {
            this.type = type; this.userId = userId;
            this.name = name; this.email = email;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // ── Fault Recovery con assign+seek ───────────────────────────────────────
    // assign+seek permite leer un topic desde el inicio hasta el final
    // de forma determinista, sin depender del rebalance de grupos.
    // Es la estrategia correcta para recovery: leer todo el historial
    // y reconstruir el estado en memoria.

    private void recoverState() {
        System.out.println("[AccountService] Recuperando estado desde '" + USER_EVENTS_TOPIC + "'...");

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "as-recovery-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(USER_EVENTS_TOPIC);
            if (partitions == null || partitions.isEmpty()) {
                System.out.println("[AccountService] Estado recuperado: 0 usuarios.");
                return;
            }

            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : partitions)
                tps.add(new TopicPartition(pi.topic(), pi.partition()));

            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            if (endOffsets.values().stream().noneMatch(o -> o > 0)) {
                System.out.println("[AccountService] Estado recuperado: 0 usuarios.");
                return;
            }

            Map<TopicPartition, Long> current = new HashMap<>();
            tps.forEach(tp -> current.put(tp, 0L));

            while (!reachedEnd(current, endOffsets)) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.of(3, ChronoUnit.SECONDS));
                for (ConsumerRecord<String, String> record : records) {
                    applyEvent(gson.fromJson(record.value(), UserEvent.class));
                    current.put(new TopicPartition(record.topic(), record.partition()),
                            record.offset() + 1);
                }
            }
        }

        System.out.println("[AccountService] Estado recuperado: " + users.size() + " usuarios.");
    }

    private boolean reachedEnd(Map<TopicPartition, Long> current, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet())
            if (current.getOrDefault(e.getKey(), 0L) < e.getValue()) return false;
        return true;
    }

    private void applyEvent(UserEvent event) {
        switch (event.type) {
            case "UserRegistered": case "UserUpdated":
                users.put(event.userId, new User(event.userId, event.name, event.email)); break;
            case "UserDeleted":
                users.remove(event.userId); break;
        }
    }

    // ── Producer ─────────────────────────────────────────────────────────────

    private KafkaProducer<String, String> createProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_ADDR);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void publish(UserEvent event) {
        try (KafkaProducer<String, String> p = createProducer()) {
            p.send(new ProducerRecord<>(USER_EVENTS_TOPIC, event.userId, gson.toJson(event)),
                (m, ex) -> { if (ex != null) System.err.println(ex.getMessage());
                             else System.out.println("[AccountService] Evento publicado -> offset=" + m.offset()); });
            p.flush();
        }
    }

    // ── Comandos ─────────────────────────────────────────────────────────────

    public void registerUser(String userId, String name, String email) {
        if (users.containsKey(userId)) {
            System.out.println("[AccountService] ERROR: Usuario '" + userId + "' ya existe."); return;
        }
        UserEvent event = new UserEvent("UserRegistered", userId, name, email);
        publish(event);
        applyEvent(event);
        System.out.println("[AccountService] Usuario registrado: " + userId);
    }

    public void updateUser(String userId, String name, String email) {
        if (!users.containsKey(userId)) {
            System.out.println("[AccountService] ERROR: Usuario '" + userId + "' no existe."); return;
        }
        UserEvent event = new UserEvent("UserUpdated", userId, name, email);
        publish(event);
        applyEvent(event);
        System.out.println("[AccountService] Usuario actualizado: " + userId);
    }

    public void deleteUser(String userId) {
        if (!users.containsKey(userId)) {
            System.out.println("[AccountService] ERROR: Usuario '" + userId + "' no existe."); return;
        }
        UserEvent event = new UserEvent("UserDeleted", userId, null, null);
        publish(event);
        applyEvent(event);
        System.out.println("[AccountService] Usuario eliminado: " + userId);
    }

    public void listUsers() {
        if (users.isEmpty()) System.out.println("[AccountService] No hay usuarios.");
        else {
            System.out.println("[AccountService] Usuarios (" + users.size() + "):");
            users.values().forEach(u -> System.out.println(
                "  - " + u.userId + " | " + u.name + " | " + u.email));
        }
    }

    // ── Main ─────────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        AccountService service = new AccountService();
        service.recoverState();
        if (args.length == 0) { printUsage(); return; }
        switch (args[0]) {
            case "register": if (args.length < 4) { printUsage(); return; }
                service.registerUser(args[1], args[2], args[3]); break;
            case "update": if (args.length < 4) { printUsage(); return; }
                service.updateUser(args[1], args[2], args[3]); break;
            case "delete": if (args.length < 2) { printUsage(); return; }
                service.deleteUser(args[1]); break;
            case "list": service.listUsers(); break;
            default: printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("Uso:");
        System.out.println("  java AccountService register <userId> <name> <email>");
        System.out.println("  java AccountService update   <userId> <name> <email>");
        System.out.println("  java AccountService delete   <userId>");
        System.out.println("  java AccountService list");
    }
}