#!/bin/bash
# Entrypoint init script for the official clickhouse-server Docker image.
#
# The clickhouse-server image auto-runs every top-level *.sh and *.sql[.gz] file found
# directly in /docker-entrypoint-initdb.d/ (in lexical/filename order) on first container
# startup (i.e. only when the data directory is empty). It does NOT recurse into
# subdirectories on its own, so this script exists to explicitly apply the migration files
# under migrations/ in numeric order via `clickhouse-client --multiquery`.
#
# Expected docker-compose mount (owned by the compose-writing agent, not this one):
#   volumes:
#     - ./infra/clickhouse/init-db.sh:/docker-entrypoint-initdb.d/init-db.sh:ro
#     - ./infra/clickhouse/migrations:/docker-entrypoint-initdb.d/migrations:ro
#
# Each migration file under migrations/ is also self-contained and valid to run
# standalone (each starts with `CREATE DATABASE IF NOT EXISTS market_data;` and uses
# fully-qualified `market_data.<table>` names), so mounting migrations/ directly as
# /docker-entrypoint-initdb.d/ (flattened, no init-db.sh) also works and still applies in
# filename order (001_, 002_, 003_, 004_) -- this script is the extra-robust variant that
# doesn't rely on that auto-scan reaching a subdirectory.

set -e

MIGRATIONS_DIR="$(dirname "$0")/migrations"

for f in "${MIGRATIONS_DIR}"/*.sql; do
    echo "Applying migration: ${f}"
    clickhouse-client --multiquery --queries-file "${f}"
done
