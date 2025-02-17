#!/bin/bash

set -e

echo "Stopping PostgreSQL containers..."
docker stop pg_primary pg_replica

echo "Removing data directories..."
docker exec -it pg_primary bash -c "rm -rf /var/lib/postgresql/data/*"
docker exec -it pg_replica bash -c "rm -rf /var/lib/postgresql/data/*"

echo "Restarting PostgreSQL containers..."
docker start pg_primary pg_replica

echo "Waiting for PostgreSQL to start..."
sleep 10

echo "Clusters reset completed!"
