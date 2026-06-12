-- DRIVER: mysql
CREATE TABLE IF NOT EXISTS microbus_flows (
    flow_id              BIGINT       NOT NULL AUTO_INCREMENT,
    flow_token           CHAR(16)     NOT NULL,
    workflow_name        VARCHAR(512) NOT NULL,
    graph                JSON         NOT NULL,
    actor_claims         JSON         NOT NULL DEFAULT ('{}'),
    status               CHAR(16)     NOT NULL,
    step_id              BIGINT       NOT NULL DEFAULT 0,
    forked_flow_id       BIGINT       NOT NULL,
    forked_step_depth    INT          NOT NULL,
    surgraph_flow_id     BIGINT       NOT NULL DEFAULT 0,
    surgraph_step_depth  INT          NOT NULL DEFAULT 0,
    thread_id            BIGINT       NOT NULL DEFAULT 0,
    thread_token         CHAR(16)     NOT NULL DEFAULT '',
    trace_parent         VARCHAR(128) NOT NULL DEFAULT '',
    notify_hostname      VARCHAR(256) NOT NULL DEFAULT '',
    final_state          TEXT         NOT NULL DEFAULT ('{}'),
    breakpoints          JSON         NOT NULL DEFAULT ('{}'),
    created_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (flow_id),
    INDEX idx_microbus_flows_status (status, updated_at),
    INDEX idx_microbus_flows_workflow_name (workflow_name),
    INDEX idx_microbus_flows_surgraph (surgraph_flow_id),
    INDEX idx_microbus_flows_thread (thread_id, flow_id)
);

-- DRIVER: pgx
CREATE TABLE IF NOT EXISTS microbus_flows (
    flow_id              BIGSERIAL    NOT NULL,
    flow_token           CHAR(16)     NOT NULL,
    workflow_name        VARCHAR(512) NOT NULL,
    graph                JSONB        NOT NULL,
    actor_claims         JSONB        NOT NULL DEFAULT '{}',
    status               CHAR(16)     NOT NULL,
    step_id              BIGINT       NOT NULL DEFAULT 0,
    forked_flow_id       BIGINT       NOT NULL,
    forked_step_depth    INT          NOT NULL,
    surgraph_flow_id     BIGINT       NOT NULL DEFAULT 0,
    surgraph_step_depth  INT          NOT NULL DEFAULT 0,
    thread_id            BIGINT       NOT NULL DEFAULT 0,
    thread_token         CHAR(16)     NOT NULL DEFAULT '',
    trace_parent         VARCHAR(128) NOT NULL DEFAULT '',
    notify_hostname      VARCHAR(256) NOT NULL DEFAULT '',
    final_state          TEXT         NOT NULL DEFAULT '{}',
    breakpoints          JSONB        NOT NULL DEFAULT '{}',
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (flow_id)
);

-- DRIVER: pgx
CREATE INDEX idx_microbus_flows_status ON microbus_flows (status, updated_at);

-- DRIVER: pgx
CREATE INDEX idx_microbus_flows_workflow_name ON microbus_flows (workflow_name);

-- DRIVER: pgx
CREATE INDEX idx_microbus_flows_surgraph ON microbus_flows (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: pgx
CREATE INDEX idx_microbus_flows_thread ON microbus_flows (thread_id, flow_id);

-- DRIVER: mssql
CREATE TABLE microbus_flows (
    flow_id              BIGINT        NOT NULL IDENTITY(1,1),
    flow_token           NCHAR(16)     NOT NULL,
    workflow_name        NVARCHAR(512) NOT NULL,
    graph                NVARCHAR(MAX) NOT NULL,
    actor_claims         NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    status               NCHAR(16)     NOT NULL,
    step_id              BIGINT        NOT NULL DEFAULT 0,
    forked_flow_id       BIGINT        NOT NULL,
    forked_step_depth    INT           NOT NULL,
    surgraph_flow_id     BIGINT        NOT NULL DEFAULT 0,
    surgraph_step_depth  INT           NOT NULL DEFAULT 0,
    thread_id            BIGINT        NOT NULL DEFAULT 0,
    thread_token         NCHAR(16)     NOT NULL DEFAULT '',
    trace_parent         NVARCHAR(128) NOT NULL DEFAULT '',
    notify_hostname      NVARCHAR(256) NOT NULL DEFAULT '',
    final_state          NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    breakpoints          NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    created_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (flow_id)
);

-- DRIVER: mssql
CREATE INDEX idx_microbus_flows_status ON microbus_flows (status, updated_at);

-- DRIVER: mssql
CREATE INDEX idx_microbus_flows_workflow_name ON microbus_flows (workflow_name);

-- DRIVER: mssql
CREATE INDEX idx_microbus_flows_surgraph ON microbus_flows (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: mssql
CREATE INDEX idx_microbus_flows_thread ON microbus_flows (thread_id, flow_id);

-- DRIVER: sqlite
CREATE TABLE IF NOT EXISTS microbus_flows (
    flow_id              INTEGER      NOT NULL PRIMARY KEY AUTOINCREMENT,
    flow_token           TEXT         NOT NULL,
    workflow_name        TEXT         NOT NULL,
    graph                TEXT         NOT NULL,
    actor_claims         TEXT         NOT NULL DEFAULT '{}',
    status               TEXT         NOT NULL,
    step_id              INTEGER      NOT NULL DEFAULT 0,
    forked_flow_id       INTEGER      NOT NULL,
    forked_step_depth    INTEGER      NOT NULL,
    surgraph_flow_id     INTEGER      NOT NULL DEFAULT 0,
    surgraph_step_depth  INTEGER      NOT NULL DEFAULT 0,
    thread_id            INTEGER      NOT NULL DEFAULT 0,
    thread_token         TEXT         NOT NULL DEFAULT '',
    trace_parent         TEXT         NOT NULL DEFAULT '',
    notify_hostname      TEXT         NOT NULL DEFAULT '',
    final_state          TEXT         NOT NULL DEFAULT '{}',
    breakpoints          TEXT         NOT NULL DEFAULT '{}',
    created_at           DATETIME     NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME     NOT NULL DEFAULT NOW_UTC()
);

-- DRIVER: sqlite
CREATE INDEX idx_microbus_flows_status ON microbus_flows (status, updated_at);

-- DRIVER: sqlite
CREATE INDEX idx_microbus_flows_workflow_name ON microbus_flows (workflow_name);

-- DRIVER: sqlite
CREATE INDEX idx_microbus_flows_surgraph ON microbus_flows (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: sqlite
CREATE INDEX idx_microbus_flows_thread ON microbus_flows (thread_id, flow_id);
