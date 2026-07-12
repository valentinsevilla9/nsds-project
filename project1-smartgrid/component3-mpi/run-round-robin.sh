#!/bin/bash
# Estrategia "round-robin": los ranks de un mismo distrito se reparten
# entre hosts, forzando que el MPI_Reduce cruce la red en cada paso.
#
# Editar HOST_LIST con los hostnames/IPs reales de nuestros portátiles.
#
# Uso: ./run-round-robin.sh [nodesPerDistrict] [simSteps] [numDistricts]

set -e
NODES_PER_DISTRICT=${1:-4}
SIM_STEPS=${2:-10}
NUM_DISTRICTS=${3:-2}
NP=$((NODES_PER_DISTRICT * NUM_DISTRICTS))

HOST_LIST=(hostA hostB)

HOSTS=""
for ((i = 0; i < NP; i++)); do
    HOSTS="$HOSTS${HOST_LIST[$((i % ${#HOST_LIST[@]}))]},"
done
HOSTS="${HOSTS%,}"

echo "Strategy: round-robin | hosts=$HOSTS | np=$NP"
mpirun -H "$HOSTS" -np "$NP" --oversubscribe ./district_simulation "$NODES_PER_DISTRICT" "$SIM_STEPS"
