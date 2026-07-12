# NSDS Project 2025/2026 — Group 34
**Networked Software for Distributed Systems — Politecnico di Milano**

---

## Projects

**#1 Smart Power Grid** — modela una red eléctrica dividida en distritos, con tres componentes que miran el mismo problema desde ángulos distintos: microservicios event-driven sobre Kafka, streaming analytics con Spark, y una simulación distribuida con MPI.

**#2 Digital Twin** — mantiene, en actores Akka, una réplica del estado de una red IoT simulada en Cooja (Contiki-NG), coordinada a través de Node-RED.

---

## Repository Structure

```
nsds-project/
├── scripts/                        ← Scripts de utilidad
│   ├── setup.sh                    ← Instala dependencias
│   └── start-env.sh                ← Arranca Kafka nativo
│
├── project1-smartgrid/
│   ├── component1-kafka/           ← 5 microservicios event-driven
│   ├── component2-spark/           ← Streaming analytics con windowing
│   └── component3-mpi/             ← Simulación distribuida de distritos en MPI
│
├── project2-digitaltwin/
│   ├── contiki-ng/                 ← Código C para nodos IoT en Cooja
│   ├── serial-bridge/              ← Puente serial de Cooja ↔ MQTT
│   ├── akka-actors/                ← Actores réplica del estado IoT
│   └── node-red/                   ← Flujos de coordinación JSON
│
Documentos de diseño
```
