-- DRIVER: mysql
ALTER TABLE microbus_steps
    ADD COLUMN parked TINYINT NOT NULL DEFAULT 0;

-- DRIVER: mysql
DROP INDEX idx_microbus_steps_selection ON microbus_steps;

-- DRIVER: mysql
DROP INDEX idx_microbus_steps_saturation ON microbus_steps;

-- DRIVER: mysql
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, parked, priority, fairness_key);

-- DRIVER: mysql
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, parked, task_name);

-- DRIVER: pgx
ALTER TABLE microbus_steps
    ADD COLUMN parked SMALLINT NOT NULL DEFAULT 0;

-- DRIVER: pgx
DROP INDEX idx_microbus_steps_selection;

-- DRIVER: pgx
DROP INDEX idx_microbus_steps_saturation;

-- DRIVER: pgx
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, parked, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: pgx
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, parked, task_name) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
ALTER TABLE microbus_steps ADD parked TINYINT NOT NULL DEFAULT 0;

-- DRIVER: mssql
DROP INDEX idx_microbus_steps_selection ON microbus_steps;

-- DRIVER: mssql
DROP INDEX idx_microbus_steps_saturation ON microbus_steps;

-- DRIVER: mssql
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, parked, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, parked, task_name) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN parked INTEGER NOT NULL DEFAULT 0;

-- DRIVER: sqlite
DROP INDEX idx_microbus_steps_selection;

-- DRIVER: sqlite
DROP INDEX idx_microbus_steps_saturation;

-- DRIVER: sqlite
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, parked, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, parked, task_name) WHERE status IN ('pending', 'running');
