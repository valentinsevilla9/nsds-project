# NSDS Project 2025/2026 — Group XX
**Networked Software for Distributed Systems — Politecnico di Milano**

## Group Members
- Student 1 (PersonaA) — Contiki-NG / Node-RED
- Student 2 (PersonaB) — Kafka Microservices
- Student 3 (PersonaC) — Spark Streaming / Akka

---

## Projects Overview

| Project | Technologies | Status |
|---|---|---|
| #1 Smart Power Grid | Kafka · Spark Streaming · MPI | 🔄 In Progress |
| #2 Digital Twin | Contiki-NG · Akka · Node-RED | 🔄 In Progress |

---

## Repository Structure

```
nsds-project/
├── scripts/                        ← Scripts de utilidad
│   ├── setup.sh                    ← Instala dependencias (ejecutar primero)
│   └── start-env.sh                ← Arranca Kafka nativo
│
├── project1-smartgrid/
│   ├── component1-kafka/           ← 5 microservicios event-driven (Persona B)
│   ├── component2-spark/           ← Streaming analytics con windowing (Persona C)
│   └── component3-mpi/             ← Simulación distribuida de distritos en MPI (todos)
│
├── project2-digitaltwin/
│   ├── contiki-ng/                 ← Código C para nodos IoT en Cooja (Persona A)
│   ├── serial-bridge/              ← Puente serial de Cooja ↔ MQTT (Persona A)
│   ├── akka-actors/                ← Actores réplica del estado IoT (Persona C)
│   └── node-red/                   ← Flujos de coordinación JSON (Persona A/B)
│
Documentos de diseño (P1 y P2) enviados por email antes de la discusión,
no versionados en este repositorio.
```

---

## Quick Start

### 1. Levantar el entorno (Kafka nativo)
```bash
./scripts/setup.sh       # comprueba/instala dependencias (una vez)
./scripts/start-env.sh   # arranca Kafka en primer plano (Ctrl+C para pararlo)
```
- Kafka disponible en `localhost:9092`

### 2. Compilar y ejecutar Component 1 (Kafka)
```bash
cd project1-smartgrid/component1-kafka
mvn clean package
# el jar tiene 5 servicios distintos, no una clase principal fija:
# hay que indicar la clase con -cp, "java -jar" solo no funciona
java -cp target/component1-kafka-1.0-jar-with-dependencies.jar smartgrid.kafka.AccountService list
```
Las 5 clases son `AccountService`, `DistrictNodeManager`, `MeasurementService`, `BillingService` y `PresentationService` (todas en el paquete `smartgrid.kafka`). Cada una imprime su uso si se ejecuta sin argumentos.

Por defecto todas usan `localhost:9092`. Para apuntar a otra máquina (por ejemplo, con Tailscale) sin recompilar, exporta `KAFKA_BROKER` antes de ejecutar:
```bash
export KAFKA_BROKER=100.x.x.x:9092
```

### 3. Node-RED + serial-bridge
```bash
node-red
# Abrir http://localhost:1880
# Importar el fichero JSON desde project2-digitaltwin/node-red/

cd project2-digitaltwin/serial-bridge
npm install
# Editar nodes.json con los puertos del Serial Socket de Cooja y las IPv6 reales
node bridge.js
```
Requiere un broker MQTT corriendo en `localhost:1883` (p. ej. Mosquitto) y, para que el cambio de período llegue al nodo real, un border router de Cooja con `tunslip6` conectado al host.

### 4. Simulación MPI (Componente 3)
```bash
cd project1-smartgrid/component3-mpi
make
# Estrategia por distrito: cada distrito en el mismo host
./run-by-district.sh [nodesPerDistrict] [simSteps] [numDistricts]
# Estrategia round-robin: nodos de un distrito repartidos entre hosts
./run-round-robin.sh [nodesPerDistrict] [simSteps] [numDistricts]
```
- Edita `HOST_LIST` dentro de cada script con los hostnames/IPs reales de vuestras máquinas.
- Compara la sección `=== Resumen: tiempo total en MPI_Reduce por rank (ms) ===` entre ambas estrategias, en distritos densos y dispersos, para el análisis de overhead de comunicación que pide el enunciado.

### 5. Parar el entorno
Ctrl+C en la terminal de `start-env.sh` (y en la de `node bridge.js`/`node-red` si están corriendo).

---

## Git Workflow

- `main` — solo código que funciona y ha sido revisado
- `feature/kafka-services` — rama de Persona B
- `feature/contiki-rpl` — rama de Persona A  
- `feature/spark-streaming` — rama de Persona C

**Regla de oro:** nunca hacer push directamente a `main`. Abrir una PR y que al menos otro miembro la revise.

---

## Deadlines

| Fecha | Tarea |
|---|---|
| 30 junio 2026 | Enviar documentos de diseño (4 pág cada uno) |
| 2 julio 2026 | Presentación y defensa con los profesores |
