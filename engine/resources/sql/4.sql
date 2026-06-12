-- DRIVER: mysql
CREATE INDEX idx_microbus_flows_created_at ON microbus_flows (created_at);

-- DRIVER: mysql
CREATE INDEX idx_microbus_steps_created_at ON microbus_steps (created_at);

-- DRIVER: pgx
CREATE INDEX idx_microbus_flows_created_at ON microbus_flows (created_at);

-- DRIVER: pgx
CREATE INDEX idx_microbus_steps_created_at ON microbus_steps (created_at);

-- DRIVER: mssql
CREATE INDEX idx_microbus_flows_created_at ON microbus_flows (created_at);

-- DRIVER: mssql
CREATE INDEX idx_microbus_steps_created_at ON microbus_steps (created_at);

-- DRIVER: sqlite
CREATE INDEX idx_microbus_flows_created_at ON microbus_flows (created_at);

-- DRIVER: sqlite
CREATE INDEX idx_microbus_steps_created_at ON microbus_steps (created_at);
