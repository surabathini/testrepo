#!/bin/bash

set -e 

# PostgreSQL Credentials
PG_PRIMARY="postgres://postgres:password@pg_primary:5432/testdb"
PG_REPLICA="postgres://postgres:password@pg_replica:5433/testdb"

echo "Setting up logical replication..."

# # 1. Enable WAL logging on primary
# echo "Enabling WAL logging on primary..."
# docker exec -it pg_primary psql -U postgres -d testdb -c "ALTER SYSTEM SET wal_level = 'logical';"
# docker exec -it pg_primary psql -U postgres -d testdb -c "ALTER SYSTEM SET max_replication_slots = 10;"
# docker exec -it pg_primary psql -U postgres -d testdb -c "ALTER SYSTEM SET max_wal_senders = 10;"
# docker exec -it pg_primary psql -U postgres -d testdb -c "SELECT pg_reload_conf();"

# 2. Create a publication on primary
echo "Creating publication on primary..."
docker exec -it pg_primary psql -U postgres -d testdb -c "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'mypub') THEN
        CREATE PUBLICATION mypub;
    END IF;
END
\$\$;"

echo "Adding tables to publication..."
docker exec -it pg_primary psql -U postgres -d testdb -c "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'mytable1') THEN
        CREATE TABLE mytable1 (id SERIAL PRIMARY KEY, name TEXT);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = 'mypub' AND tablename = 'mytable1') THEN
        ALTER PUBLICATION mypub ADD TABLE mytable1;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'mytable2') THEN
        CREATE TABLE mytable2 (id SERIAL PRIMARY KEY, description TEXT);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = 'mypub' AND tablename = 'mytable2') THEN
        ALTER PUBLICATION mypub ADD TABLE mytable2;
    END IF;
END
\$\$;"

# 3. Allow replication connection from replica
echo "Configuring pg_hba.conf..."
docker exec -it pg_primary bash -c "echo 'host replication postgres 0.0.0.0/0 trust' >> /var/lib/postgresql/data/pg_hba.conf"
docker exec -it pg_primary bash -c "echo 'host all postgres 0.0.0.0/0 trust' >> /var/lib/postgresql/data/pg_hba.conf"
docker exec -it pg_primary psql -U postgres -d testdb -c "SELECT pg_reload_conf();"

# 4. Create tables on replica and setup subscription
echo "Creating tables on replica..."
docker exec -it pg_replica psql -U postgres -d testdb -c "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'mytable1') THEN
        CREATE TABLE mytable1 (id SERIAL PRIMARY KEY, name TEXT);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'mytable2') THEN
        CREATE TABLE mytable2 (id SERIAL PRIMARY KEY, description TEXT);
    END IF;
END
\$\$;"

echo "Checking if subscription exists on replica..."
SUBSCRIPTION_EXISTS=$(docker exec -it pg_replica psql -U postgres -d testdb -tAc "SELECT 1 FROM pg_subscription WHERE subname = 'mysub'")

if [ "$SUBSCRIPTION_EXISTS" != "1" ]; then
    echo "Creating subscription on replica..."
    docker exec -it pg_replica psql -U postgres -d testdb -c "
    CREATE SUBSCRIPTION mysub CONNECTION '$PG_PRIMARY' PUBLICATION mypub WITH (create_slot = true);"
else
    echo "Subscription already exists on replica."
fi

echo "Replication setup completed!"
