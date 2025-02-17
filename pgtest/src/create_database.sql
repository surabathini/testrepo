DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_database
        WHERE datname = 'mydatabase'
    ) THEN
        CREATE DATABASE mydatabase;
    END IF;
END $$;