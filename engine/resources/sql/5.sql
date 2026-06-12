-- DRIVER: mysql
ALTER TABLE microbus_steps
    ADD COLUMN lineage_id      BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN cohort_size     INT    NOT NULL DEFAULT 0,
    ADD COLUMN cohort_arrivals INT    NOT NULL DEFAULT 0;

-- DRIVER: pgx
ALTER TABLE microbus_steps
    ADD COLUMN lineage_id      BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN cohort_size     INT    NOT NULL DEFAULT 0,
    ADD COLUMN cohort_arrivals INT    NOT NULL DEFAULT 0;

-- DRIVER: mssql
ALTER TABLE microbus_steps ADD
    lineage_id      BIGINT NOT NULL DEFAULT 0,
    cohort_size     INT    NOT NULL DEFAULT 0,
    cohort_arrivals INT    NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN lineage_id INTEGER NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN cohort_size INTEGER NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN cohort_arrivals INTEGER NOT NULL DEFAULT 0;
