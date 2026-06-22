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
| #1 Smart Power Grid | Kafka · Spark Streaming · Akka | 🔄 In Progress |
| #2 Digital Twin | Contiki-NG · Akka · Node-RED | 🔄 In Progress |

---

## Repository Structure

```
nsds-project/
├── docker-compose.yml              ← Arranca Kafka + Spark con un comando
├── scripts/                        ← Scripts de utilidad
│   ├── setup.sh                    ← Instala dependencias (ejecutar primero)
│   └── start-env.sh                ← Arranca el entorno completo
│
├── project1-smartgrid/
│   ├── component1-kafka/           ← 5 microservicios event-driven (Persona B)
│   ├── component2-spark/           ← Streaming analytics con windowing (Persona C)
│   └── component3-simulation/      ← Simulación distribuida Akka (todos)
│
├── project2-digitaltwin/
│   ├── contiki-ng/                 ← Código C para nodos IoT en Cooja (Persona A)
│   ├── akka-actors/                ← Actores réplica del estado IoT (Persona C)
│   └── node-red/                   ← Flujos de coordinación JSON (Persona A/B)
│
└── docs/
    ├── design-project1.md          ← Documento diseño P1 (entregar el 30/06)
    └── design-project2.md          ← Documento diseño P2 (entregar el 30/06)
```

---

## Quick Start

### 1. Levantar el entorno (Kafka + Spark)
```bash
docker-compose up -d
```
- Kafka disponible en `localhost:9092`
- Kafka UI (ver topics/mensajes) en http://localhost:8090
- Spark Master UI en http://localhost:8080

### 2. Compilar un componente Java
```bash
cd project1-smartgrid/component1-kafka
mvn clean package
java -jar target/component1-kafka-1.0-jar-with-dependencies.jar
```

### 3. Node-RED
```bash
node-red
# Abrir http://localhost:1880
# Importar el fichero JSON desde project2-digitaltwin/node-red/
```

### 4. Parar el entorno
```bash
docker-compose down
```

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
