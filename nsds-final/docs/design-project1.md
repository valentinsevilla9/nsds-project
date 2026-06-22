# Design Document — Project #1: Distributed Smart Power Grid
**NSDS 2025/2026 — Group XX**
*Politecnico di Milano — Luca Mottola, Alessandro Margara*

---

## 1. System Architecture Overview

*(~0.5 páginas) Describe la arquitectura global del sistema, los tres componentes y cómo se relacionan entre ellos. Incluir un diagrama simple.*

El sistema se compone de tres capas independientes que se comunican a través de topics Kafka:

- **Component 1 (Grid Management Layer):** arquitectura de microservicios event-driven basada en Kafka.
- **Component 2 (Streaming Analytics):** job Spark Structured Streaming que consume el topic de mediciones.
- **Component 3 (Distributed Simulation):** simulación distribuida con actores Akka.

---

## 2. Component 1 — Kafka Microservices

*(~1 página)*

### Servicios implementados
| Servicio | Topics producidos | Topics consumidos |
|---|---|---|
| AccountService | `user-events` | — |
| DistrictNodeManager | `node-events` | `user-events` |
| MeasurementService | `measurements` | `node-events` |
| BillingService | `billing-records` | `measurements` |
| PresentationService | — | `node-events`, `billing-records` |

### Fault Recovery
Los servicios no comparten estado. Al reiniciarse, cada servicio replays los eventos desde el inicio del topic Kafka asignado a su consumer group, reconstruyendo el estado en memoria.

### Garantías de entrega
Se usa `enable.auto.commit=false` con commit manual tras procesar cada mensaje, garantizando at-least-once delivery.

---

## 3. Component 2 — Spark Streaming Analytics

*(~0.5 páginas)*

El job lee del topic `measurements` usando el conector `spark-sql-kafka`. Se implementan dos queries:

1. **Agregación por distrito:** `groupBy(districtId)` con estado acumulado usando `mapGroupsWithState`.
2. **Ventana temporal deslizante:** `window(timestamp, "5 minutes", "1 minute")` sobre el balance energético.

Se usa event-time semantics con un watermark de 30 segundos para tolerar eventos fuera de orden.

---

## 4. Component 3 — Akka Distributed Simulation

*(~0.5 páginas)*

Cada nodo de la grid (producer, consumer, accumulator) se modela como un actor Akka. Se comparan dos estrategias de distribución:

- **Estrategia A (por distrito):** todos los actores de un distrito en el mismo proceso. Minimiza comunicación inter-proceso para distritos densos.
- **Estrategia B (round-robin):** actores distribuidos uniformemente entre procesos. Mejor escalabilidad para distritos esparcidos.

### Resultados experimentales
*(completar con mediciones reales)*

| Escenario | Estrategia A | Estrategia B |
|---|---|---|
| Distrito denso (100 nodos) | X ms | Y ms |
| Distrito disperso (5 nodos) | X ms | Y ms |

---

## 5. Design Choices & Trade-offs

- Se eligió **Spark Structured Streaming** sobre DStreams por su mejor soporte de event-time y SQL API.
- Los microservicios usan **in-memory state** (HashMap) por simplicidad; la durabilidad la provee Kafka.
- La simulación Akka usa **mensajes inmutables** para evitar condiciones de carrera.
