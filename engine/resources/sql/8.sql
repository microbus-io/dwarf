-- DRIVER: mysql
ALTER TABLE microbus_flows
    ADD COLUMN priority        INT          NOT NULL DEFAULT 5,
    ADD COLUMN fairness_key    VARCHAR(256) NOT NULL DEFAULT '',
    ADD COLUMN fairness_weight DOUBLE       NOT NULL DEFAULT 1;

-- DRIVER: mysql
ALTER TABLE microbus_steps
    ADD COLUMN priority        INT          NOT NULL DEFAULT 5,
    ADD COLUMN fairness_key    VARCHAR(256) NOT NULL DEFAULT '',
    ADD COLUMN fairness_weight DOUBLE       NOT NULL DEFAULT 1;

-- DRIVER: mysql
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, priority, fairness_key);

-- DRIVER: mysql
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, task_name);

-- DRIVER: pgx
ALTER TABLE microbus_flows
    ADD COLUMN priority        INT              NOT NULL DEFAULT 5,
    ADD COLUMN fairness_key    VARCHAR(256)     NOT NULL DEFAULT '',
    ADD COLUMN fairness_weight DOUBLE PRECISION NOT NULL DEFAULT 1;

-- DRIVER: pgx
ALTER TABLE microbus_steps
    ADD COLUMN priority        INT              NOT NULL DEFAULT 5,
    ADD COLUMN fairness_key    VARCHAR(256)     NOT NULL DEFAULT '',
    ADD COLUMN fairness_weight DOUBLE PRECISION NOT NULL DEFAULT 1;

-- DRIVER: pgx
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: pgx
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, task_name) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
ALTER TABLE microbus_flows ADD
    priority        INT           NOT NULL DEFAULT 5,
    fairness_key    NVARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight FLOAT         NOT NULL DEFAULT 1;

-- DRIVER: mssql
ALTER TABLE microbus_steps ADD
    priority        INT           NOT NULL DEFAULT 5,
    fairness_key    NVARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight FLOAT         NOT NULL DEFAULT 1;

-- DRIVER: mssql
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, task_name) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN priority INTEGER NOT NULL DEFAULT 5;

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN fairness_key TEXT NOT NULL DEFAULT '';

-- DRIVER: sqlite
ALTER TABLE microbus_flows ADD COLUMN fairness_weight REAL NOT NULL DEFAULT 1;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN priority INTEGER NOT NULL DEFAULT 5;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN fairness_key TEXT NOT NULL DEFAULT '';

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN fairness_weight REAL NOT NULL DEFAULT 1;

-- DRIVER: sqlite
CREATE INDEX idx_microbus_steps_selection ON microbus_steps (status, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
CREATE INDEX idx_microbus_steps_saturation ON microbus_steps (status, task_name) WHERE status IN ('pending', 'running');
