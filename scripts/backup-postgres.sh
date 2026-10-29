#!/bin/sh
# Dump the Timescale database from the compose service. Run from the repository root.
set -eu
stamp=$(TZ=Asia/Kolkata date +%Y%m%dT%H%M%S)
mkdir -p backups
docker compose exec -T postgres pg_dump -U scalper -d scalper --no-owner > "backups/scalper-${stamp}.sql"
echo "wrote backups/scalper-${stamp}.sql"
