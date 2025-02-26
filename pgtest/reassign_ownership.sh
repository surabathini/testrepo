#!/bin/bash

# Check if all arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <host_name> <old_owner> <new_owner>"
    exit 1
fi

# Export environment variables
# export PGUSER=
# export PGPASSWORD=

# Set the psql command
HOST_NAME=$1
OLD_OWNER=$2
NEW_OWNER=$3

DEFAULT_DB="postgres"
PORT=5432
export PGHOST=$HOST_NAME
PSQL_CMD="psql -d $DEFAULT_DB -p $PORT"

# Get the list of databases
DATABASES=$($PSQL_CMD -t -A -c "SELECT datname FROM pg_database WHERE datistemplate = false;")

# Loop through each database
for DB in $DATABASES; do
    echo "Reassigning ownership in database: $DB"
    
    # Connect to the database and reassign ownership
    $PSQL_CMD -d "$DB" -c "REASSIGN OWNED BY \"$OLD_OWNER\" TO \"$NEW_OWNER\";"
done