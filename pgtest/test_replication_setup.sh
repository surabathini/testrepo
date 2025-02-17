#!/bin/bash

echo "Checking replication settings on primary..."
docker exec -it pg_primary psql -U postgres -d testdb -c "SHOW wal_level;"
docker exec -it pg_primary psql -U postgres -d testdb -c "SHOW max_replication_slots;"
docker exec -it pg_primary psql -U postgres -d testdb -c "SHOW max_wal_senders;"
docker exec -it pg_primary psql -U postgres -d testdb -c "SELECT * FROM pg_replication_slots;"
docker exec -it pg_primary psql -U postgres -d testdb -c "SELECT * FROM pg_publication;"
docker exec -it pg_primary psql -U postgres -d testdb -c "SELECT * FROM pg_publication_tables WHERE pubname = 'mypub';"

echo "Checking subscription settings on replica..."
docker exec -it pg_replica psql -U postgres -d testdb -c "SHOW wal_level;"
docker exec -it pg_replica psql -U postgres -d testdb -c "SHOW max_replication_slots;"
docker exec -it pg_replica psql -U postgres -d testdb -c "SHOW max_wal_senders;"
docker exec -it pg_replica psql -U postgres -d testdb -c "SELECT * FROM pg_subscription;"
docker exec -it pg_replica psql -U postgres -d testdb -c "SELECT * FROM pg_stat_subscription;"
