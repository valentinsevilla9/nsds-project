#!/bin/bash
# Estrategia "by-district": cada distrito completo se coloca en el mismo
# host, asi el MPI_Reduce intra-distrito se queda en memoria compartida.
#
# Las IPs salen de scripts/team.env (copiad team.env.example y rellenad
# las vuestras). Si no existe ese archivo, usa hostA/hostB/hostC de
# ejemplo, que no van a funcionar de verdad.
#
# Uso: ./run-by-district.sh [nodesPerDistrict] [simSteps] [numDistricts] [numHosts]
# numHosts: cuantos portatiles de team.env usar (por defecto los 3).
# Poned 2 para probar con solo dos maquinas (usa Persona A y Persona B).

set -e
NODES_PER_DISTRICT=${1:-4}
SIM_STEPS=${2:-10}
NUM_DISTRICTS=${3:-2}
NUM_HOSTS=${4:-3}

TEAM_ENV="$(dirname "$0")/../../scripts/team.env"
if [ -f "$TEAM_ENV" ]; then
    source "$TEAM_ENV"
    HOST_LIST=("$PERSONA_A_IP" "$PERSONA_B_IP" "$PERSONA_C_IP")
else
    echo "Aviso: no encuentro scripts/team.env, uso hostnames de ejemplo (no funcionara de verdad)."
    echo "Copiad scripts/team.env.example a scripts/team.env y rellenad vuestras IPs de Tailscale."
    HOST_LIST=(hostA hostB hostC)
fi
HOST_LIST=("${HOST_LIST[@]:0:$NUM_HOSTS}")

HOSTS=""
for ((i = 0; i < NUM_DISTRICTS; i++)); do
    HOSTS="$HOSTS${HOST_LIST[$((i % ${#HOST_LIST[@]}))]}:${NODES_PER_DISTRICT},"
done
HOSTS="${HOSTS%,}"
NP=$((NODES_PER_DISTRICT * NUM_DISTRICTS))

echo "Strategy: by-district | hosts=$HOSTS | np=$NP"
mpirun -H "$HOSTS" -np "$NP" --oversubscribe ./district_simulation "$NODES_PER_DISTRICT" "$SIM_STEPS"
