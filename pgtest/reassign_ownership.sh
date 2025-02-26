#!/bin/bash

# Set the psql command
export PGUSER=
export PGPASSWORD=
DEFAULT_DB="postgres"
PORT=5432
PSQL_CMD="psql -d $DEFAULT_DB -p $PORT"

# Set the old and new owner
OLD_OWNER="a"
NEW_OWNER="b"

# Get the list of databases
DATABASES=$($PSQL_CMD -t -A -c "SELECT datname FROM pg_database WHERE datistemplate = false;")

# Loop through each database
for DB in $DATABASES; do
    echo "Reassigning ownership in database: $DB"
    
    # Connect to the database and reassign ownership
    $PSQL_CMD -d "$DB" -c "REASSIGN OWNED BY \"$OLD_OWNER\" TO \"$NEW_OWNER\";"
done