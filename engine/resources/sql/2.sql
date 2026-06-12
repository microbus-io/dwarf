-- DRIVER: mysql
CREATE TABLE IF NOT EXISTS microbus_steps (
    step_id            BIGINT       NOT NULL AUTO_INCREMENT,
    flow_id            BIGINT       NOT NULL,
    step_depth         INT          NOT NULL,
    step_token         CHAR(16)     NOT NULL,
    task_name          VARCHAR(512) NOT NULL,
    state              JSON         NOT NULL DEFAULT ('{}'),
    changes            JSON         NOT NULL DEFAULT ('{}'),
    interrupt_payload  JSON         NOT NULL DEFAULT ('{}'),
    status             CHAR(16)     NOT NULL,
    goto_next          VARCHAR(512) NOT NULL DEFAULT '',
    error              TEXT         NOT NULL DEFAULT (''),
    time_budget_ms     INT          NOT NULL,
    breakpoint_hit     TINYINT      NOT NULL DEFAULT 0,
    attempt            INT          NOT NULL DEFAULT 0,
    not_before         DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    lease_expires      DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    created_at         DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at         DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (step_id),
    INDEX idx_microbus_steps_flow_id (flow_id, step_id),
    INDEX idx_microbus_steps_status (status, updated_at)
);

-- DRIVER: pgx
CREATE TABLE IF NOT EXISTS microbus_steps (
    step_id            BIGSERIAL    NOT NULL,
    flow_id            BIGINT       NOT NULL,
    step_depth         INT          NOT NULL,
    step_token         CHAR(16)     NOT NULL,
    task_name          VARCHAR(512) NOT NULL,
    state              JSONB        NOT NULL DEFAULT '{}',
    changes            JSONB        NOT NULL DEFAULT '{}',
    interrupt_payload  JSONB        NOT NULL DEFAULT '{}',
    status             CHAR(16)     NOT NULL,
    goto_next          VARCHAR(512) NOT NULL DEFAULT '',
    error              TEXT         NOT NULL DEFAULT '',
    time_budget_ms     INT          NOT NULL,
    breakpoint_hit     SMALLINT     NOT NULL DEFAULT 0,
    attempt            INT          NOT NULL DEFAULT 0,
    not_before         TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    lease_expires      TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (step_id)
);

-- DRIVER: pgx
CREATE INDEX idx_microbus_steps_flow_id ON microbus_steps (flow_id);

-- DRIVER: pgx
CREATE INDEX idx_microbus_steps_status ON microbus_steps (status, updated_at) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
CREATE TABLE microbus_steps (
    step_id            BIGINT        NOT NULL IDENTITY(1,1),
    flow_id            BIGINT        NOT NULL,
    step_depth         INT           NOT NULL,
    step_token         NCHAR(16)     NOT NULL,
    task_name          NVARCHAR(512) NOT NULL,
    state              NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    changes            NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    interrupt_payload  NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    status             NCHAR(16)     NOT NULL,
    goto_next          NVARCHAR(512) NOT NULL DEFAULT '',
    error              NVARCHAR(MAX) NOT NULL DEFAULT '',
    time_budget_ms     INT           NOT NULL,
    breakpoint_hit     TINYINT       NOT NULL DEFAULT 0,
    attempt            INT           NOT NULL DEFAULT 0,
    not_before         DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    lease_expires      DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    created_at         DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at         DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (step_id)
);

-- DRIVER: mssql
CREATE INDEX idx_microbus_steps_flow_id ON microbus_steps (flow_id);

-- DRIVER: mssql
CREATE INDEX idx_microbus_steps_status ON microbus_steps (status, updated_at) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
CREATE TABLE IF NOT EXISTS microbus_steps (
    step_id            INTEGER      NOT NULL PRIMARY KEY AUTOINCREMENT,
    flow_id            INTEGER      NOT NULL,
    step_depth         INTEGER      NOT NULL,
    step_token         TEXT         NOT NULL,
    task_name          TEXT         NOT NULL,
    state              TEXT         NOT NULL DEFAULT '{}',
    changes            TEXT         NOT NULL DEFAULT '{}',
    interrupt_payload  TEXT         NOT NULL DEFAULT '{}',
    status             TEXT         NOT NULL,
    goto_next          TEXT         NOT NULL DEFAULT '',
    error              TEXT         NOT NULL DEFAULT '',
    time_budget_ms     INTEGER      NOT NULL,
    breakpoint_hit     INTEGER      NOT NULL DEFAULT 0,
    attempt            INTEGER      NOT NULL DEFAULT 0,
    not_before         DATETIME     NOT NULL DEFAULT NOW_UTC(),
    lease_expires      DATETIME     NOT NULL DEFAULT NOW_UTC(),
    created_at         DATETIME     NOT NULL DEFAULT NOW_UTC(),
    updated_at         DATETIME     NOT NULL DEFAULT NOW_UTC()
);

-- DRIVER: sqlite
CREATE INDEX idx_microbus_steps_flow_id ON microbus_steps (flow_id, step_id);

-- DRIVER: sqlite
CREATE INDEX idx_microbus_steps_status ON microbus_steps (status, updated_at) WHERE status IN ('pending', 'running');
