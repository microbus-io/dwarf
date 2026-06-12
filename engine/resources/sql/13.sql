-- DRIVER: mysql
ALTER TABLE microbus_steps
    ADD COLUMN cohort_failures INT NOT NULL DEFAULT 0;

-- DRIVER: pgx
ALTER TABLE microbus_steps
    ADD COLUMN cohort_failures INT NOT NULL DEFAULT 0;

-- DRIVER: mssql
ALTER TABLE microbus_steps ADD cohort_failures INT NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN cohort_failures INTEGER NOT NULL DEFAULT 0;
