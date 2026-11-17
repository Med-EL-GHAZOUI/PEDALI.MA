#!/usr/bin/env bash
# Tests Partie 1 (Personne 2) : MinIO, bucket lake, Jupyter.
# Usage : bash tests/check_storage.sh
set -u
ok=0; ko=0
# Ports MinIO : lus dans docker/.env (defaut 9000/9001)
ENV_FILE="$(dirname "$0")/../docker/.env"
API_PORT=$(grep -s '^MINIO_API_PORT=' "$ENV_FILE" | cut -d= -f2); API_PORT=${API_PORT:-9000}
CONSOLE_PORT=$(grep -s '^MINIO_CONSOLE_PORT=' "$ENV_FILE" | cut -d= -f2); CONSOLE_PORT=${CONSOLE_PORT:-9001}
check() { if eval "$2" >/dev/null 2>&1; then echo "[OK]  $1"; ok=$((ok+1)); else echo "[KO]  $1"; ko=$((ko+1)); fi; }

check "MinIO API en bonne sante ($API_PORT)"   "curl -sf http://localhost:$API_PORT/minio/health/live"
check "Console MinIO accessible ($CONSOLE_PORT)"   "curl -sf -o /dev/null http://localhost:$CONSOLE_PORT"
check "Bucket 'lake' existe"              "docker run --rm --network pedali-net -e MC_HOST_l=http://minioadmin:minioadmin123@minio:9000 cgr.dev/chainguard/minio-client:latest ls l | grep -q lake"
check "Jupyter accessible (8888)"         "curl -sf -o /dev/null http://localhost:8888/api"
check "Jupyter joint le reseau pedali-net" "docker inspect pedali-jupyter --format '{{json .NetworkSettings.Networks}}' | grep -q pedali-net"
check "Jupyter voit MinIO"                "docker exec pedali-jupyter python -c \"import urllib.request;urllib.request.urlopen('http://minio:9000/minio/health/live')\""
check "Jupyter voit spark-master:7077"    "docker exec pedali-jupyter python -c \"import socket;socket.create_connection(('spark-master',7077),3)\""

echo "Resultat : $ok OK, $ko KO"
[ "$ko" -eq 0 ]
