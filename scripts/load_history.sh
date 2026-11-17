#!/usr/bin/env bash
# Copie data/trips_history.jsonl dans MinIO : s3a://lake/bronze/history/ (Partie 2 du sujet).
# Usage : bash scripts/load_history.sh   (cluster Docker demarre, reseau pedali-net)
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
USER_="${MINIO_ROOT_USER:-minioadmin}"; PASS_="${MINIO_ROOT_PASSWORD:-minioadmin123}"
MSYS_NO_PATHCONV=1 docker run --rm --network pedali-net -v "$ROOT/data:/data:ro" \
  -e MC_HOST_l="http://$USER_:$PASS_@pedali-minio:9000" cgr.dev/chainguard/minio-client:latest \
  cp /data/trips_history.jsonl /data/stations.json l/lake/bronze/history/
MSYS_NO_PATHCONV=1 docker run --rm --network pedali-net \
  -e MC_HOST_l="http://$USER_:$PASS_@pedali-minio:9000" cgr.dev/chainguard/minio-client:latest ls l/lake/bronze/history/
