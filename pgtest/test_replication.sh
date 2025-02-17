#!/bin/bash

echo "Inserting data into primary database..."
docker exec -it pg_primary psql -U postgres -d testdb -c "
INSERT INTO mytable1 (name) VALUES ('Alice'), ('Bob');
INSERT INTO mytable2 (description) VALUES ('Test Data 1'), ('Test Data 2');"

sleep 10  # Wait for replication

echo "Checking data on replica..."
docker exec -it pg_replica psql -U postgres -d testdb -c "SELECT * FROM mytable1;"
docker exec -it pg_replica psql -U postgres -d testdb -c "SELECT * FROM mytable2;"
