#!/bin/bash
# start-env.sh - arranca el broker Kafka nativo en primer plano
# (Ctrl+C para pararlo).
#
# Usando Tailscale (varios portatiles), hay que tener puesto en config/server.properties, 
# antes de lanzar esto:
#   advertised.listeners=PLAINTEXT://<vuestra-ip-tailscale>:9092
# Si no, los otros portátiles se conectan pero luego Kafka les dice
# que se reconecten a localhost y falla.

set -e

# se puede sobreescribir sin tocar el script: KAFKA_DIR=/otra/ruta ./start-env.sh
KAFKA_DIR="${KAFKA_DIR:-$HOME/Desktop/kafka_2.13-4.1.0}"

if [ ! -x "$KAFKA_DIR/bin/kafka-server-start.sh" ]; then
    echo "No encuentro Kafka en: $KAFKA_DIR"
    echo "Si lo tenéis instalado en otro sitio, exportad KAFKA_DIR antes de ejecutar este script."
    exit 1
fi

echo "Arrancando Kafka desde $KAFKA_DIR..."
"$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
