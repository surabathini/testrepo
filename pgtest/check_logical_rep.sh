#!/bin/bash

set -e

# Identify if the server is a PRIMARY or SUBSCRIBER
server_role=$(docker exec pg_primary psql -U postgres -d testdb -tAc "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_publication) THEN 'primary' ELSE 'subscriber' END;")

echo "Detected Server Role: $server_role"

if [ "$server_role" == "primary" ]; then
    echo "Running checks for PRIMARY (Publishing) Server..."
    
    # Check if wal_level is set to 'logical'
    echo "Checking wal_level setting..."
    wal_level=$(docker exec pg_primary psql -U postgres -d testdb -tAc "SHOW wal_level;")
    if [ "$wal_level" != "logical" ]; then
        echo "wal_level is set to '$wal_level'. It should be 'logical' for logical replication."
        exit 1
    else
        echo "wal_level is correctly set to 'logical'."
    fi

    # List all publications
    echo "Checking publications..."
    docker exec pg_primary psql -U postgres -d testdb -tAc "SELECT * FROM pg_publication;"

    # Check replication slots
    echo "Checking replication slots..."
    docker exec pg_primary psql -U postgres -d testdb -tAc "SELECT * FROM pg_replication_slots;"
fi

server_role="subscriber"

if [ "$server_role" == "subscriber" ]; then

    echo "Running checks for SUBSCRIBER (Replica) Server..."
    
    # List all subscriptions
    echo "Checking subscriptions..."
    docker exec pg_replica psql -U postgres -d testdb -tAc "SELECT * FROM pg_subscription;"

    # Check for active replication connections
    echo "Checking active replication connections..."
    docker exec pg_replica psql -U postgres -d testdb -tAc "SELECT * FROM pg_stat_subscription;"
    
else
    echo "No logical replication setup detected on this server."
    exit 1
fi

# # Display recent logs for replication issues (optional)
# echo "Fetching recent PostgreSQL logs (last 50 lines)..."
# sudo journalctl -u postgresql --no-pager -n 50 2>/dev/null || echo "Log access may require sudo permissions."

echo "Logical replication check completed."
