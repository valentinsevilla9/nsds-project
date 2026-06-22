# Design Document — Project #2: Digital Twin for a Low-power Wireless Network
**NSDS 2025/2026 — Group XX**
*Politecnico di Milano — Luca Mottola, Alessandro Margara*

---

## 1. System Architecture Overview

*(~0.5 páginas) Describe la arquitectura de tres capas y la separación de responsabilidades.*

El sistema está compuesto por tres capas claramente separadas:

- **Contiki-NG / Cooja:** red IoT simulada con protocolo RPL. Los nodos envían tráfico periódico al root y reportan cambios de padre.
- **Node-RED:** componente de coordinación central. Recibe eventos de la red IoT y actualiza los actores Akka, y viceversa.
- **Akka Actors:** réplica virtual (digital twin) de cada nodo IoT.

*Esta separación cumple el requisito del enunciado: la lógica de control del Digital Twin reside exclusivamente en Node-RED, no en Contiki-NG ni en Akka.*

---

## 2. Contiki-NG Network

*(~0.5 páginas)*

### Topología
Red simulada en Cooja con N nodos mote (sky) y un nodo root (border router). Se usa RPL con DODAG para el árbol de enrutamiento.

### Eventos generados por los nodos
Cada nodo reporta por el puerto serie los siguientes eventos:
- `PARENT_CHANGE:<nodeId>:<newParentId>` — cambio de padre en el árbol RPL
- `MSG_SENT:<nodeId>:<seqNum>` — envío de mensaje periódico al root
- `NODE_CRASH:<nodeId>` — simulación de caída (proceso terminado en Cooja)

### Configuración del período T
Los nodos escuchan por UDP comandos de reconfiguración del período, que llegan desde Node-RED.

---

## 3. Node-RED Coordination Layer

*(~0.5 páginas)*

Node-RED actúa como middleware entre Contiki-NG y Akka. Implementa los siguientes flujos:

| Flujo | Entrada | Acción |
|---|---|---|
| Parent sync | Serial output de Cooja | HTTP POST al actor Akka correspondiente |
| Message mirror | Serial output de Cooja | Trigger de mensaje entre actores Akka |
| Period update | HTTP desde Akka | UDP al nodo IoT en Cooja |
| Crash handling | Serial output de Cooja | Notificación de crash al actor Akka |

*Swapping de lógica: cambiar el flujo Node-RED es suficiente para cambiar la lógica del Digital Twin sin tocar Contiki-NG ni Akka.*

---

## 4. Akka Digital Twin Actors

*(~0.5 páginas)*

Un actor por nodo IoT. El estado de cada actor contiene:
- `currentParent: String` — padre actual en el árbol RPL
- `parentHistory: LinkedList<String>` — últimos K padres
- `period: int` — período de generación de mensajes T (en ms)

### Sincronización
- Cambio de padre → mensaje `ParentUpdateMsg` recibido desde Node-RED vía HTTP
- Mensaje de aplicación → mensaje `AppMsg` entre actores, coordinado por Node-RED
- Cambio de T → el actor notifica a Node-RED, que lo propaga al nodo IoT
- Crash de nodo → el actor recibe `CrashMsg`, activa supervisor y dispara recovery en Contiki-NG

---

## 5. Design Choices & Trade-offs

- **Node-RED como único coordinador:** garantiza la separación limpia exigida por el enunciado. Akka y Contiki-NG no se comunican directamente.
- **HTTP para Akka ↔ Node-RED:** simplicidad e interoperabilidad. Alternativa MQTT descartada por añadir un broker extra.
- **Serial console de Cooja** para extraer eventos de los nodos: solución nativa y sin modificar el stack de red.
