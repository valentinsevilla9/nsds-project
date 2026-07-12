#!/bin/bash
# setup.sh - comprueba (e instala si falta) todo lo necesario para Proyecto 1.
# Compatible con macOS y Linux (Ubuntu/Debian). En Windows, correr esto
# dentro de WSL, no en PowerShell.

set -e
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo ""
echo "  NSDS Project - Setup de dependencias"
echo ""

check() {
    if command -v "$1" &> /dev/null; then
        echo -e "${GREEN}✓ $1 encontrado: $(command -v "$1")${NC}"
        return 0
    else
        echo -e "${RED}✗ $1 NO encontrado${NC}"
        return 1
    fi
}

echo ""
echo "[ Java 21 ]"
if check java; then
    VERSION=$(java -version 2>&1 | head -1)
    echo "  Versión: $VERSION"
else
    echo -e "${YELLOW}  → Instala Java 21 desde: https://adoptium.net/${NC}"
    echo "macOS:  brew install openjdk@21"
    echo "Ubuntu: sudo apt install openjdk-21-jdk"
fi

echo ""
echo "[ Maven ]"
if check mvn; then
    mvn -version | head -1
else
    echo -e "${YELLOW}  → macOS:  brew install maven${NC}"
    echo "Ubuntu: sudo apt install maven"
fi

echo ""
echo "[ Git ]"
if check git; then
    git --version
    echo "  Usuario: $(git config user.name 2>/dev/null || echo 'NO configurado')"
    echo "  Email:   $(git config user.email 2>/dev/null || echo 'NO configurado')"
    if [ -z "$(git config user.name)" ]; then
        echo -e "${YELLOW}  → Configura Git:${NC}"
        echo "    git config --global user.name 'Tu Nombre'"
        echo "    git config --global user.email 'tu@email.com'"
    fi
fi

echo ""
echo "[ Node.js + Node-RED ]"
if check node; then
    node --version
    if check node-red; then
        node-red --version
    else
        echo -e "${YELLOW}  → Instala Node-RED: npm install -g --unsafe-perm node-red${NC}"
    fi
else
    echo -e "${YELLOW}  → Instala Node.js 18+ desde: https://nodejs.org/${NC}"
    echo "macOS:  brew install node"
    echo "Ubuntu: sudo apt install nodejs npm"
fi

echo ""
echo "[ MPI ]"
# Comprobamos los dos por separado (no con "check mpicc && check
# mpirun"), si no en cuanto falte uno el otro ni se llega a mirar.
# Y "check X && OK=0" en vez de "check X; OK=$?" porque con set -e activo,
# un comando suelto que falla corta el script - dentro de un && no pasa.
MPICC_OK=1
MPIRUN_OK=1
check mpicc && MPICC_OK=0
check mpirun && MPIRUN_OK=0
if [ $MPICC_OK -eq 0 ] && [ $MPIRUN_OK -eq 0 ]; then
    mpirun --version | head -1
else
    echo -e "${YELLOW}  → Instala OpenMPI:${NC}"
    echo "macOS:   brew install open-mpi"
    echo "Ubuntu:  sudo apt install openmpi-bin libopenmpi-dev"
    echo "Windows: no hay instalación nativa limpia, usa WSL y ejecuta este script ahí dentro"
fi

echo ""
echo "  Siguiente paso: ejecuta ./scripts/start-env.sh"
echo "  para arrancar Kafka."
echo ""
