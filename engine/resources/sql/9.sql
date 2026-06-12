-- DRIVER: mysql
ALTER TABLE microbus_flows
    ADD COLUMN error         TEXT NOT NULL DEFAULT (''),
    ADD COLUMN cancel_reason TEXT NOT NULL DEFAULT (''),
    ADD COLUMN tenant_id     INT  NOT NULL DEFAULT 0;

-- DRIVER: pgx
ALTER TABLE microbus_flows
    ADD COLUMN error         TEXT NOT NULL DEFAULT '',
    ADD COLUMN cancel_reason TEXT NOT NULL DEFAULT '',
    ADD COLUMN tenant_id     INT  NOT NULL DEFAULT 0;

-- DRIVER: mssql
ALTER TABLE microbus_flows ADD
    error         NVARCHAR(MAX) NOT NULL DEFAULT '',
    cancel_reason NVARCHAR(MAX) NOT NULL DEFAULT '',
    tenant_id     INT           NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN error TEXT NOT NULL DEFAULT '';

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN cancel_reason TEXT NOT NULL DEFAULT '';

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN tenant_id INTEGER NOT NULL DEFAULT 0;
