#!/bin/bash
# Arranca Kafka nativo (instalado en ~/Desktop/kafka_2.13-4.1.0)
KAFKA_DIR="$HOME/Desktop/kafka_2.13-4.1.0"
echo "Arrancando Kafka 4.1.0..."
$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties