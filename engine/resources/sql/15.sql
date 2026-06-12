-- DRIVER: mysql
ALTER TABLE microbus_flows
    ADD COLUMN started_at DATETIME(3) NOT NULL DEFAULT NOW_UTC();

-- DRIVER: mysql
UPDATE microbus_flows SET started_at = created_at;

-- DRIVER: pgx
ALTER TABLE microbus_flows
    ADD COLUMN started_at TIMESTAMPTZ NOT NULL DEFAULT NOW_UTC();

-- DRIVER: pgx
UPDATE microbus_flows SET started_at = created_at;

-- DRIVER: mssql
ALTER TABLE microbus_flows ADD started_at DATETIME2(3) NOT NULL DEFAULT NOW_UTC();

-- DRIVER: mssql
UPDATE microbus_flows SET started_at = created_at;

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN started_at DATETIME NOT NULL DEFAULT NOW_UTC();

-- DRIVER: sqlite
UPDATE microbus_flows SET started_at = created_at;
