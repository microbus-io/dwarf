-- DRIVER: mysql
ALTER TABLE microbus_steps
    ADD COLUMN predecessor_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN successor_id   BIGINT NOT NULL DEFAULT 0;

-- DRIVER: pgx
ALTER TABLE microbus_steps
    ADD COLUMN predecessor_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN successor_id   BIGINT NOT NULL DEFAULT 0;

-- DRIVER: mssql
ALTER TABLE microbus_steps ADD
    predecessor_id BIGINT NOT NULL DEFAULT 0,
    successor_id   BIGINT NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN predecessor_id INTEGER NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN successor_id INTEGER NOT NULL DEFAULT 0;
