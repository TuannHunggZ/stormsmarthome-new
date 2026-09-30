CREATE TABLE IF NOT EXISTS import_status (
    finished BOOLEAN NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

TRUNCATE import_status;
INSERT INTO import_status (finished) VALUES (TRUE);

DO $$
BEGIN
    RAISE NOTICE 'Topology PostgreSQL preload completed.';
END $$;
