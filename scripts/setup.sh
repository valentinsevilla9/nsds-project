#!/bin/bash
# ============================================================
# setup.sh — Verifica e instala todas las dependencias
# Ejecutar UNA VEZ en cada portátil del grupo
# Compatible con macOS y Linux (Ubuntu/Debian)
# ============================================================

set -e
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "================================================"
echo "  NSDS Project — Setup de dependencias"
echo "================================================"

check() {
    if command -v $1 &> /dev/null; then
        echo -e "${GREEN}✓ $1 encontrado: $(command -v $1)${NC}"
        return 0
    else
        echo -e "${RED}✗ $1 NO encontrado${NC}"
        return 1
    fi
}

# ── Java 17 ──────────────────────────────────────────────────
echo ""
echo "[ Java 17 ]"
if check java; then
    VERSION=$(java -version 2>&1 | head -1)
    echo "  Versión: $VERSION"
else
    echo -e "${YELLOW}  → Instala Java 17 desde: https://adoptium.net/${NC}"
    echo "    macOS:  brew install openjdk@17"
    echo "    Ubuntu: sudo apt install openjdk-17-jdk"
fi

# ── Maven ────────────────────────────────────────────────────
echo ""
echo "[ Maven ]"
if check mvn; then
    mvn -version | head -1
else
    echo -e "${YELLOW}  → macOS:  brew install maven${NC}"
    echo "    Ubuntu: sudo apt install maven"
fi

# ── Git ──────────────────────────────────────────────────────
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

# ── Node.js + Node-RED ───────────────────────────────────────
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
    echo "    macOS:  brew install node"
    echo "    Ubuntu: sudo apt install nodejs npm"
fi

# ── MPI (OpenMPI/MPICH) ───────────────────────────────────────
echo ""
echo "[ MPI ]"
if check mpicc && check mpirun; then
    mpirun --version | head -1
else
    echo -e "${YELLOW}  → Instala OpenMPI:${NC}"
    echo "    macOS:  brew install open-mpi"
    echo "    Ubuntu: sudo apt install openmpi-bin libopenmpi-dev"
fi

# ── Resumen final ────────────────────────────────────────────
echo ""
echo "================================================"
echo "  Siguiente paso: ejecuta ./scripts/start-env.sh"
echo "  para arrancar Kafka y Spark con Docker."
echo "================================================"
