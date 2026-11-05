-- Copyright (c) 2026 Microbus LLC and various contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- 	http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- DRIVER: mysql
CREATE TABLE IF NOT EXISTS dwarf_flows_archive (
    flow_id              BIGINT       NOT NULL,
    flow_token           VARCHAR(16)  NOT NULL,
    workflow_url         VARCHAR(512) NOT NULL,
    workflow_name        VARCHAR(512) NOT NULL,
    graph                JSON         NOT NULL,
    graph_hash           VARCHAR(64)  NOT NULL,
    payload_ver          INT          NOT NULL DEFAULT 1,
    baggage              JSON         NOT NULL DEFAULT ('{}'),
    status               VARCHAR(16)  NOT NULL,
    stopped_step_id      BIGINT       NOT NULL DEFAULT 0,
    surgraph_flow_id     BIGINT       NOT NULL DEFAULT 0,
    surgraph_step_id     BIGINT       NOT NULL DEFAULT 0,
    root_flow_id         BIGINT       NOT NULL DEFAULT 0,
    forked_from_step     BIGINT       NOT NULL DEFAULT 0,
    trace_parent         VARCHAR(128) NOT NULL DEFAULT '',
    delete_on_completion TINYINT      NOT NULL DEFAULT 0,
    delete_after_ms      INT          NOT NULL DEFAULT 0,
    final_state          JSON         NOT NULL DEFAULT ('{}'),
    error                TEXT         NOT NULL DEFAULT (''),
    cancel_reason        TEXT         NOT NULL DEFAULT (''),
    cancel_watermark     BIGINT       NOT NULL DEFAULT 0,
    priority             INT          NOT NULL DEFAULT 5,
    fairness_key         VARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight      DOUBLE       NOT NULL DEFAULT 1,
    time_budget_ms       INT          NOT NULL DEFAULT 0,
    engine_id            BIGINT       NOT NULL DEFAULT 0,
    created_hour         INT          NOT NULL DEFAULT (FLOOR(UNIX_TIMESTAMP() / 3600) - 262968),
    created_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    started_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    touch                TINYINT      NOT NULL DEFAULT 0,
    archived_at          DATETIME(3)  NOT NULL DEFAULT '9999-12-31 23:59:59',
    PRIMARY KEY (flow_id),
    INDEX idx_dwarf_flows_archive_status (status, archived_at),
    INDEX idx_dwarf_flows_archive_workflow_url (workflow_url),
    INDEX idx_dwarf_flows_archive_surgraph (surgraph_flow_id),
    INDEX idx_dwarf_flows_archive_surgraph_step (surgraph_step_id),
    INDEX idx_dwarf_flows_archive_root (root_flow_id),
    INDEX idx_dwarf_flows_archive_archived (archived_at)
);

-- DRIVER: pgx
CREATE TABLE IF NOT EXISTS dwarf_flows_archive (
    flow_id              BIGINT           NOT NULL,
    flow_token           VARCHAR(16)      NOT NULL,
    workflow_url         VARCHAR(512)     NOT NULL,
    workflow_name        VARCHAR(512)     NOT NULL,
    graph                JSONB            NOT NULL,
    graph_hash           VARCHAR(64)      NOT NULL,
    payload_ver          INT              NOT NULL DEFAULT 1,
    baggage              JSONB            NOT NULL DEFAULT '{}',
    status               VARCHAR(16)      NOT NULL,
    stopped_step_id      BIGINT           NOT NULL DEFAULT 0,
    surgraph_flow_id     BIGINT           NOT NULL DEFAULT 0,
    surgraph_step_id     BIGINT           NOT NULL DEFAULT 0,
    root_flow_id         BIGINT           NOT NULL DEFAULT 0,
    forked_from_step     BIGINT           NOT NULL DEFAULT 0,
    trace_parent         VARCHAR(128)     NOT NULL DEFAULT '',
    delete_on_completion SMALLINT         NOT NULL DEFAULT 0,
    delete_after_ms      INT              NOT NULL DEFAULT 0,
    final_state          TEXT             NOT NULL DEFAULT '{}',
    error                TEXT             NOT NULL DEFAULT '',
    cancel_reason        TEXT             NOT NULL DEFAULT '',
    cancel_watermark     BIGINT           NOT NULL DEFAULT 0,
    priority             INT              NOT NULL DEFAULT 5,
    fairness_key         VARCHAR(256)     NOT NULL DEFAULT '',
    fairness_weight      DOUBLE PRECISION NOT NULL DEFAULT 1,
    time_budget_ms       INT              NOT NULL DEFAULT 0,
    engine_id            BIGINT           NOT NULL DEFAULT 0,
    created_hour         INT              NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM NOW()) / 3600) - 262968),
    created_at           TIMESTAMP        NOT NULL DEFAULT NOW_UTC(),
    started_at           TIMESTAMP        NOT NULL DEFAULT NOW_UTC(),
    updated_at           TIMESTAMP        NOT NULL DEFAULT NOW_UTC(),
    touch                SMALLINT         NOT NULL DEFAULT 0,
    archived_at          TIMESTAMP        NOT NULL DEFAULT '9999-12-31 23:59:59',
    PRIMARY KEY (flow_id)
);

-- DRIVER: pgx
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_status ON dwarf_flows_archive (status, archived_at);

-- DRIVER: pgx
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_workflow_url ON dwarf_flows_archive (workflow_url);

-- DRIVER: pgx
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_surgraph ON dwarf_flows_archive (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: pgx
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_surgraph_step ON dwarf_flows_archive (surgraph_step_id) WHERE surgraph_step_id > 0;

-- DRIVER: pgx
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_root ON dwarf_flows_archive (root_flow_id);

-- DRIVER: pgx
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_archived ON dwarf_flows_archive (archived_at);

-- DRIVER: pgx
ALTER TABLE dwarf_flows_archive SET (autovacuum_freeze_min_age = 0, autovacuum_vacuum_insert_scale_factor = 0.01, toast.autovacuum_freeze_min_age = 0, toast.autovacuum_vacuum_insert_scale_factor = 0.01);

-- DRIVER: mssql
IF OBJECT_ID(N'dwarf_flows_archive', N'U') IS NULL CREATE TABLE dwarf_flows_archive (
    flow_id              BIGINT        NOT NULL,
    flow_token           NVARCHAR(16)  NOT NULL,
    workflow_url         NVARCHAR(512) NOT NULL,
    workflow_name        NVARCHAR(512) NOT NULL,
    graph                VARBINARY(MAX) NOT NULL,
    graph_hash           NVARCHAR(64)  NOT NULL,
    payload_ver          INT           NOT NULL DEFAULT 1,
    baggage              VARBINARY(MAX) NOT NULL DEFAULT 0x7B7D,
    status               NVARCHAR(16)  NOT NULL,
    stopped_step_id      BIGINT        NOT NULL DEFAULT 0,
    surgraph_flow_id     BIGINT        NOT NULL DEFAULT 0,
    surgraph_step_id     BIGINT        NOT NULL DEFAULT 0,
    root_flow_id         BIGINT        NOT NULL DEFAULT 0,
    forked_from_step     BIGINT        NOT NULL DEFAULT 0,
    trace_parent         NVARCHAR(128) NOT NULL DEFAULT '',
    delete_on_completion TINYINT       NOT NULL DEFAULT 0,
    delete_after_ms      INT           NOT NULL DEFAULT 0,
    final_state          VARBINARY(MAX) NOT NULL DEFAULT 0x7B7D,
    error                NVARCHAR(MAX) NOT NULL DEFAULT '',
    cancel_reason        NVARCHAR(MAX) NOT NULL DEFAULT '',
    cancel_watermark     BIGINT        NOT NULL DEFAULT 0,
    priority             INT           NOT NULL DEFAULT 5,
    fairness_key         NVARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight      FLOAT         NOT NULL DEFAULT 1,
    time_budget_ms       INT           NOT NULL DEFAULT 0,
    engine_id            BIGINT        NOT NULL DEFAULT 0,
    created_hour         INT           NOT NULL DEFAULT (DATEDIFF(HOUR, '2000-01-01T00:00:00', SYSUTCDATETIME())),
    created_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    started_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    touch                TINYINT       NOT NULL DEFAULT 0,
    archived_at          DATETIME2(3)  NOT NULL DEFAULT '9999-12-31 23:59:59',
    PRIMARY KEY (flow_id)
);

-- DRIVER: mssql
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_dwarf_flows_archive_status' AND object_id = OBJECT_ID(N'dwarf_flows_archive')) CREATE INDEX idx_dwarf_flows_archive_status ON dwarf_flows_archive (status, archived_at);

-- DRIVER: mssql
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_dwarf_flows_archive_workflow_url' AND object_id = OBJECT_ID(N'dwarf_flows_archive')) CREATE INDEX idx_dwarf_flows_archive_workflow_url ON dwarf_flows_archive (workflow_url);

-- DRIVER: mssql
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_dwarf_flows_archive_surgraph' AND object_id = OBJECT_ID(N'dwarf_flows_archive')) CREATE INDEX idx_dwarf_flows_archive_surgraph ON dwarf_flows_archive (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: mssql
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_dwarf_flows_archive_surgraph_step' AND object_id = OBJECT_ID(N'dwarf_flows_archive')) CREATE INDEX idx_dwarf_flows_archive_surgraph_step ON dwarf_flows_archive (surgraph_step_id) WHERE surgraph_step_id > 0;

-- DRIVER: mssql
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_dwarf_flows_archive_root' AND object_id = OBJECT_ID(N'dwarf_flows_archive')) CREATE INDEX idx_dwarf_flows_archive_root ON dwarf_flows_archive (root_flow_id);

-- DRIVER: mssql
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_dwarf_flows_archive_archived' AND object_id = OBJECT_ID(N'dwarf_flows_archive')) CREATE INDEX idx_dwarf_flows_archive_archived ON dwarf_flows_archive (archived_at);

-- DRIVER: sqlite
CREATE TABLE IF NOT EXISTS dwarf_flows_archive (
    flow_id              INTEGER  NOT NULL PRIMARY KEY,
    flow_token           TEXT     NOT NULL,
    workflow_url         TEXT     NOT NULL,
    workflow_name        TEXT     NOT NULL,
    graph                TEXT     NOT NULL,
    graph_hash           TEXT     NOT NULL,
    payload_ver          INTEGER  NOT NULL DEFAULT 1,
    baggage              TEXT     NOT NULL DEFAULT '{}',
    status               TEXT     NOT NULL,
    stopped_step_id      INTEGER  NOT NULL DEFAULT 0,
    surgraph_flow_id     INTEGER  NOT NULL DEFAULT 0,
    surgraph_step_id     INTEGER  NOT NULL DEFAULT 0,
    root_flow_id         INTEGER  NOT NULL DEFAULT 0,
    forked_from_step     INTEGER  NOT NULL DEFAULT 0,
    trace_parent         TEXT     NOT NULL DEFAULT '',
    delete_on_completion INTEGER  NOT NULL DEFAULT 0,
    delete_after_ms      INTEGER  NOT NULL DEFAULT 0,
    final_state          TEXT     NOT NULL DEFAULT '{}',
    error                TEXT     NOT NULL DEFAULT '',
    cancel_reason        TEXT     NOT NULL DEFAULT '',
    cancel_watermark     INTEGER  NOT NULL DEFAULT 0,
    priority             INTEGER  NOT NULL DEFAULT 5,
    fairness_key         TEXT     NOT NULL DEFAULT '',
    fairness_weight      REAL     NOT NULL DEFAULT 1,
    time_budget_ms       INTEGER  NOT NULL DEFAULT 0,
    engine_id            INTEGER  NOT NULL DEFAULT 0,
    created_hour         INTEGER  NOT NULL DEFAULT ((CAST(STRFTIME('%s', 'now') AS INTEGER) / 3600) - 262968),
    created_at           DATETIME NOT NULL DEFAULT NOW_UTC(),
    started_at           DATETIME NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME NOT NULL DEFAULT NOW_UTC(),
    touch                INTEGER  NOT NULL DEFAULT 0,
    archived_at          DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59.000'
);

-- DRIVER: sqlite
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_status ON dwarf_flows_archive (status, archived_at);

-- DRIVER: sqlite
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_workflow_url ON dwarf_flows_archive (workflow_url);

-- DRIVER: sqlite
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_surgraph ON dwarf_flows_archive (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: sqlite
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_surgraph_step ON dwarf_flows_archive (surgraph_step_id) WHERE surgraph_step_id > 0;

-- DRIVER: sqlite
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_root ON dwarf_flows_archive (root_flow_id);

-- DRIVER: sqlite
CREATE INDEX IF NOT EXISTS idx_dwarf_flows_archive_archived ON dwarf_flows_archive (archived_at);
