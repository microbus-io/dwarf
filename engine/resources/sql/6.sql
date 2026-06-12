-- DRIVER: mysql
ALTER TABLE microbus_steps ADD COLUMN fan_out_ordinal INT NOT NULL DEFAULT 0;

-- DRIVER: pgx
ALTER TABLE microbus_steps ADD COLUMN fan_out_ordinal INT NOT NULL DEFAULT 0;

-- DRIVER: mssql
ALTER TABLE microbus_steps ADD fan_out_ordinal INT NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN fan_out_ordinal INTEGER NOT NULL DEFAULT 0;
