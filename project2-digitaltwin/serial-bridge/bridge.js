'use strict';

/*
 * bridge.js - puente de PROTOCOLO entre los motes Cooja y MQTT/UDP.
 * Solo traduce formatos, no decide nada por su cuenta.
 *
 * Uplink (mote -> MQTT): se conecta por TCP al Serial Socket de cada
 * mote en Cooja, parsea las lineas PARENT_CHANGE:/MSG_SENT: que emite
 * iot-node.c y las publica tal cual en iot/<nodeId>/parent y
 * iot/<nodeId>/msg.
 *
 * Downlink (MQTT -> mote): reenvia iot/<nodeId>/period como UDP
 * "PERIOD:<segundos>" a la IPv6 real del mote. Esto solo llega a la red
 * simulada si el border router de Cooja tiene tunslip6 (u equivalente)
 * conectado al host.
 *
 * Configuracion en nodes.json.
 */

const net = require('net');
const dgram = require('dgram');
const fs = require('fs');
const path = require('path');
const mqtt = require('mqtt');

const config = JSON.parse(fs.readFileSync(path.join(__dirname, 'nodes.json'), 'utf8'));

const mqttClient = mqtt.connect(config.mqttBroker);
const udpSocket = dgram.createSocket('udp6');

function log(...args) {
    console.log(new Date().toISOString(), '-', ...args);
}

function publish(topic, payload) {
    mqttClient.publish(topic, String(payload));
}

function resolveNodeIdByIpv6(ipv6) {
    const match = config.nodes.find((n) => n.ipv6 && n.ipv6.toLowerCase() === ipv6.toLowerCase());
    return match ? match.nodeId : null;
}

function handleLine(nodeId, line) {
    const parentMatch = line.match(/PARENT_CHANGE:\s*([0-9a-fA-F:]+)/);
    const msgMatch = line.match(/MSG_SENT:\s*(\d+)/);
    if (!parentMatch && !msgMatch) return;

    if (parentMatch) {
        const rawParentAddr = parentMatch[1];
        const newParentId = resolveNodeIdByIpv6(rawParentAddr) ?? rawParentAddr;
        publish(`iot/${nodeId}/parent`, newParentId);
    }
    if (msgMatch) {
        publish(`iot/${nodeId}/msg`, msgMatch[1]);
    }
}

function connectSerial(nodeCfg) {
    const nodeId = nodeCfg.nodeId;
    let buffer = '';

    const connect = () => {
        const socket = net.connect(nodeCfg.serialPort, nodeCfg.serialHost);

        socket.on('connect', () => {
            log(`Conectado al serial socket del nodo ${nodeId} (${nodeCfg.serialHost}:${nodeCfg.serialPort})`);
        });

        socket.on('data', (chunk) => {
            buffer += chunk.toString('utf8');
            let newlineIndex;
            while ((newlineIndex = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, newlineIndex);
                buffer = buffer.slice(newlineIndex + 1);
                handleLine(nodeId, line);
            }
        });

        socket.on('error', (err) => {
            log(`Error en el serial socket del nodo ${nodeId}:`, err.message);
        });

        socket.on('close', () => {
            log(`Conexion cerrada con el nodo ${nodeId}, reintentando en 5s...`);
            setTimeout(connect, 5000);
        });
    };

    connect();
}

function forwardPeriodChange(nodeId, rawPayload) {
    const nodeCfg = config.nodes.find((n) => n.nodeId === nodeId);
    if (!nodeCfg || !nodeCfg.ipv6) {
        log(`No puedo reenviar el cambio de periodo a ${nodeId}: falta ipv6 en nodes.json`);
        return;
    }

    let periodMs = Number(rawPayload);
    if (!Number.isFinite(periodMs)) {
        try {
            periodMs = Number(JSON.parse(rawPayload).periodMs);
        } catch (e) {
            periodMs = NaN;
        }
    }
    if (!Number.isFinite(periodMs) || periodMs <= 0) {
        log(`Payload de periodo invalido para ${nodeId}: ${rawPayload}`);
        return;
    }

    const periodSeconds = Math.round(periodMs / 1000);
    const message = Buffer.from(`PERIOD:${periodSeconds}`);
    udpSocket.send(message, config.udpCommandPort, nodeCfg.ipv6, (err) => {
        if (err) {
            log(`Error enviando PERIOD a ${nodeId} (${nodeCfg.ipv6}):`, err.message);
            return;
        }
        log(`PERIOD:${periodSeconds} enviado a nodo ${nodeId} (${nodeCfg.ipv6}) -- requiere border router/tunslip6 activo`);
    });
}

mqttClient.on('connect', () => {
    log('Conectado al broker MQTT en', config.mqttBroker);
    mqttClient.subscribe('iot/+/period');
});

mqttClient.on('message', (topic, payload) => {
    const parts = topic.split('/');
    if (parts[2] === 'period') {
        forwardPeriodChange(parts[1], payload.toString());
    }
});

mqttClient.on('error', (err) => log('Error MQTT:', err.message));

config.nodes.forEach(connectSerial);

process.on('SIGINT', () => {
    udpSocket.close();
    mqttClient.end(true, () => process.exit(0));
});
