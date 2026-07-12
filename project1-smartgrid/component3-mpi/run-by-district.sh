#!/bin/bash
# Estrategia "by-district": cada distrito completo se coloca en el mismo
# host, asi el MPI_Reduce intra-distrito se queda en memoria compartida.
#
# Editar HOST_LIST con los hostnames/IPs reales de nuestros portátiles
# (un host por distrito; se reutilizan en orden si hay mas distritos
# que hosts en la lista).
#
# Uso: ./run-by-district.sh [nodesPerDistrict] [simSteps] [numDistricts]

set -e
NODES_PER_DISTRICT=${1:-4}
SIM_STEPS=${2:-10}
NUM_DISTRICTS=${3:-2}
NP=$((NODES_PER_DISTRICT * NUM_DISTRICTS))

HOST_LIST=(hostA hostB)

HOSTS=""
for ((i = 0; i < NUM_DISTRICTS; i++)); do
    HOSTS="$HOSTS${HOST_LIST[$((i % ${#HOST_LIST[@]}))]}:${NODES_PER_DISTRICT},"
done
HOSTS="${HOSTS%,}"

echo "Strategy: by-district | hosts=$HOSTS | np=$NP"
mpirun -H "$HOSTS" -np "$NP" --oversubscribe ./district_simulation "$NODES_PER_DISTRICT" "$SIM_STEPS"
