DO $$ 
DECLARE 
    tbl RECORD;
    retry_count INT := 5;  -- Number of retries for lock waits
    delay INTERVAL := '2s';  -- Delay between retries
BEGIN
    -- Ensure the publication exists
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'mypub') THEN
        CREATE PUBLICATION mypub;
    END IF;

    -- Loop through all tables in the database (including schema)
    FOR tbl IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')  -- Exclude system schemas
    LOOP
        -- Retry logic for ALTER PUBLICATION in case of lock waits
        FOR i IN 1..retry_count LOOP
            BEGIN
                -- Check if the table is already part of the publication
                IF NOT EXISTS (
                    SELECT 1 FROM pg_publication_tables 
                    WHERE pubname = 'mypub' 
                    AND schemaname = tbl.schemaname
                    AND tablename = tbl.tablename
                ) THEN
                    -- Add the table to the publication with schema reference
                    EXECUTE format('ALTER PUBLICATION mypub ADD TABLE %I.%I', tbl.schemaname, tbl.tablename);
                END IF;
                
                -- Exit retry loop if successful
                EXIT;
            EXCEPTION
                WHEN lock_not_available THEN
                    RAISE NOTICE 'Lock wait detected, retrying in % seconds...', EXTRACT(EPOCH FROM delay);
                    PERFORM pg_sleep(EXTRACT(EPOCH FROM delay)); -- Wait before retrying
                WHEN OTHERS THEN
                    RAISE WARNING 'Error while adding table %I.%I to publication: %', tbl.schemaname, tbl.tablename, SQLERRM;
                    EXIT;
            END;
        END LOOP;
    END LOOP;
END $$;
