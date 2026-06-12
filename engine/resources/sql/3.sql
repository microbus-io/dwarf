-- DRIVER: mysql
ALTER TABLE microbus_flows ADD COLUMN surgraph_step_id BIGINT NOT NULL DEFAULT 0;

-- DRIVER: pgx
ALTER TABLE microbus_flows ADD COLUMN surgraph_step_id BIGINT NOT NULL DEFAULT 0;

-- DRIVER: mssql
ALTER TABLE microbus_flows ADD surgraph_step_id BIGINT NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN surgraph_step_id INTEGER NOT NULL DEFAULT 0;
