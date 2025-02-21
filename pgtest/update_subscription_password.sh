#!/bin/bash

# Configuration
SUBSCRIPTION_NAME="your_subscription_name"  # Name of the subscription
PUBLISHER_HOST="publisher_host"            # Publisher host address
PUBLISHER_PORT="5432"                      # Publisher port
DATABASE_NAME="your_database"              # Database name
REPLICATION_USER="replication_user"        # Replication user
NEW_PASSWORD="new_password"                # New password for the replication user
PUBLICATION_NAME="your_publication_name"  # Name of the publication

# Step 1: Disable the subscription
echo "Disabling subscription: $SUBSCRIPTION_NAME"
psql -c "ALTER SUBSCRIPTION $SUBSCRIPTION_NAME DISABLE;"

# Step 2: Drop the subscription (preserve the replication slot)
echo "Dropping subscription: $SUBSCRIPTION_NAME"
psql -c "DROP SUBSCRIPTION $SUBSCRIPTION_NAME;"

# Step 3: Recreate the subscription with the new password and same slot
echo "Recreating subscription: $SUBSCRIPTION_NAME with new password"
psql -c "CREATE SUBSCRIPTION $SUBSCRIPTION_NAME \
CONNECTION 'host=$PUBLISHER_HOST port=$PUBLISHER_PORT dbname=$DATABASE_NAME user=$REPLICATION_USER password=$NEW_PASSWORD' \
PUBLICATION $PUBLICATION_NAME \
WITH (slot_name = '${SUBSCRIPTION_NAME}_slot');"

# Step 4: Enable the subscription
echo "Enabling subscription: $SUBSCRIPTION_NAME"
psql -c "ALTER SUBSCRIPTION $SUBSCRIPTION_NAME ENABLE;"

# Step 5: Verify replication progress
echo "Verifying replication progress..."
echo "On Publisher:"
psql -c "SELECT * FROM pg_stat_replication;"
echo "On Subscriber:"
psql -c "SELECT * FROM pg_stat_subscription;"

echo "Subscription password updated successfully!"