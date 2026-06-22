#!/bin/bash
# ============================================================
# start-env.sh — Arranca el entorno completo del proyecto
# ============================================================

set -e
GREEN='\033[0;32m'
NC='\033[0m'

ROOT=$(cd "$(dirname "$0")/.." && pwd)

echo "Arrancando Kafka + Spark..."
cd "$ROOT"
docker-compose up -d

echo ""
echo -e "${GREEN}✓ Entorno listo:${NC}"
echo "  Kafka broker:  localhost:9092"
echo "  Kafka UI:      http://localhost:8090"
echo "  Spark Master:  http://localhost:8080"
echo ""
echo "Para parar todo: docker-compose down"
echo ""

# Espera a que Kafka esté disponible
echo "Esperando a que Kafka esté listo..."
until docker exec nsds-kafka kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 &>/dev/null; do
    printf "."
    sleep 2
done
echo ""
echo -e "${GREEN}✓ Kafka disponible. Puedes lanzar tus microservicios.${NC}"
