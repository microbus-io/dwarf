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
CREATE TABLE IF NOT EXISTS dwarf_flows (
    flow_id              BIGINT       NOT NULL AUTO_INCREMENT,
    flow_token           CHAR(16)     NOT NULL,
    workflow_url        VARCHAR(512) NOT NULL,
    workflow_name       VARCHAR(512) NOT NULL,
    graph                JSON         NOT NULL,
    baggage         JSON         NOT NULL DEFAULT ('{}'),
    status               CHAR(16)     NOT NULL,
    step_id              BIGINT       NOT NULL DEFAULT 0,
    surgraph_flow_id     BIGINT       NOT NULL DEFAULT 0,
    surgraph_step_depth  INT          NOT NULL DEFAULT 0,
    surgraph_step_id     BIGINT       NOT NULL DEFAULT 0,
    thread_id            BIGINT       NOT NULL DEFAULT 0,
    thread_token         CHAR(16)     NOT NULL DEFAULT '',
    trace_parent         VARCHAR(128) NOT NULL DEFAULT '',
    notify_on_stop       TINYINT      NOT NULL DEFAULT 0,
    final_state          TEXT         NOT NULL DEFAULT ('{}'),
    breakpoints          JSON         NOT NULL DEFAULT ('{}'),
    error                TEXT         NOT NULL DEFAULT (''),
    cancel_reason        TEXT         NOT NULL DEFAULT (''),
    priority             INT          NOT NULL DEFAULT 5,
    fairness_key         VARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight      DOUBLE       NOT NULL DEFAULT 1,
    time_budget_ms       INT          NOT NULL DEFAULT 0,
    created_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    started_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (flow_id),
    INDEX idx_dwarf_flows_status (status, updated_at),
    INDEX idx_dwarf_flows_workflow_url (workflow_url),
    INDEX idx_dwarf_flows_surgraph (surgraph_flow_id),
    INDEX idx_dwarf_flows_thread (thread_id, flow_id),
    INDEX idx_dwarf_flows_created_at (created_at)
);

-- DRIVER: pgx
CREATE TABLE IF NOT EXISTS dwarf_flows (
    flow_id              BIGSERIAL    NOT NULL,
    flow_token           CHAR(16)     NOT NULL,
    workflow_url        VARCHAR(512) NOT NULL,
    workflow_name       VARCHAR(512) NOT NULL,
    graph                JSONB        NOT NULL,
    baggage         JSONB        NOT NULL DEFAULT '{}',
    status               CHAR(16)     NOT NULL,
    step_id              BIGINT       NOT NULL DEFAULT 0,
    surgraph_flow_id     BIGINT       NOT NULL DEFAULT 0,
    surgraph_step_depth  INT          NOT NULL DEFAULT 0,
    surgraph_step_id     BIGINT       NOT NULL DEFAULT 0,
    thread_id            BIGINT       NOT NULL DEFAULT 0,
    thread_token         CHAR(16)     NOT NULL DEFAULT '',
    trace_parent         VARCHAR(128) NOT NULL DEFAULT '',
    notify_on_stop       SMALLINT     NOT NULL DEFAULT 0,
    final_state          TEXT             NOT NULL DEFAULT '{}',
    breakpoints          JSONB        NOT NULL DEFAULT '{}',
    error                TEXT             NOT NULL DEFAULT '',
    cancel_reason        TEXT             NOT NULL DEFAULT '',
    priority             INT          NOT NULL DEFAULT 5,
    fairness_key         VARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight      DOUBLE PRECISION NOT NULL DEFAULT 1,
    time_budget_ms       INT          NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    started_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (flow_id)
);

-- DRIVER: pgx
CREATE INDEX idx_dwarf_flows_status ON dwarf_flows (status, updated_at);

-- DRIVER: pgx
CREATE INDEX idx_dwarf_flows_workflow_url ON dwarf_flows (workflow_url);

-- DRIVER: pgx
CREATE INDEX idx_dwarf_flows_surgraph ON dwarf_flows (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: pgx
CREATE INDEX idx_dwarf_flows_thread ON dwarf_flows (thread_id, flow_id);

-- DRIVER: pgx
CREATE INDEX idx_dwarf_flows_created_at ON dwarf_flows (created_at);

-- DRIVER: mssql
CREATE TABLE dwarf_flows (
    flow_id              BIGINT        NOT NULL IDENTITY(1,1),
    flow_token           NCHAR(16)     NOT NULL,
    workflow_url        NVARCHAR(512) NOT NULL,
    workflow_name       NVARCHAR(512) NOT NULL,
    graph                NVARCHAR(MAX) NOT NULL,
    baggage         NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    status               NCHAR(16)     NOT NULL,
    step_id              BIGINT        NOT NULL DEFAULT 0,
    surgraph_flow_id     BIGINT        NOT NULL DEFAULT 0,
    surgraph_step_depth  INT           NOT NULL DEFAULT 0,
    surgraph_step_id     BIGINT        NOT NULL DEFAULT 0,
    thread_id            BIGINT        NOT NULL DEFAULT 0,
    thread_token         NCHAR(16)     NOT NULL DEFAULT '',
    trace_parent         NVARCHAR(128) NOT NULL DEFAULT '',
    notify_on_stop       TINYINT      NOT NULL DEFAULT 0,
    final_state          NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    breakpoints          NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    error                NVARCHAR(MAX) NOT NULL DEFAULT '',
    cancel_reason        NVARCHAR(MAX) NOT NULL DEFAULT '',
    priority             INT           NOT NULL DEFAULT 5,
    fairness_key         NVARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight      FLOAT         NOT NULL DEFAULT 1,
    time_budget_ms       INT           NOT NULL DEFAULT 0,
    created_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    started_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (flow_id)
);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_flows_status ON dwarf_flows (status, updated_at);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_flows_workflow_url ON dwarf_flows (workflow_url);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_flows_surgraph ON dwarf_flows (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: mssql
CREATE INDEX idx_dwarf_flows_thread ON dwarf_flows (thread_id, flow_id);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_flows_created_at ON dwarf_flows (created_at);

-- DRIVER: sqlite
CREATE TABLE IF NOT EXISTS dwarf_flows (
    flow_id              INTEGER      NOT NULL PRIMARY KEY AUTOINCREMENT,
    flow_token           TEXT         NOT NULL,
    workflow_url        TEXT         NOT NULL,
    workflow_name       TEXT         NOT NULL,
    graph                TEXT         NOT NULL,
    baggage         TEXT         NOT NULL DEFAULT '{}',
    status               TEXT         NOT NULL,
    step_id              INTEGER      NOT NULL DEFAULT 0,
    surgraph_flow_id     INTEGER      NOT NULL DEFAULT 0,
    surgraph_step_depth  INTEGER      NOT NULL DEFAULT 0,
    surgraph_step_id     INTEGER      NOT NULL DEFAULT 0,
    thread_id            INTEGER      NOT NULL DEFAULT 0,
    thread_token         TEXT         NOT NULL DEFAULT '',
    trace_parent         TEXT         NOT NULL DEFAULT '',
    notify_on_stop       INTEGER      NOT NULL DEFAULT 0,
    final_state          TEXT         NOT NULL DEFAULT '{}',
    breakpoints          TEXT         NOT NULL DEFAULT '{}',
    error                TEXT         NOT NULL DEFAULT '',
    cancel_reason        TEXT         NOT NULL DEFAULT '',
    priority             INTEGER      NOT NULL DEFAULT 5,
    fairness_key         TEXT         NOT NULL DEFAULT '',
    fairness_weight      REAL         NOT NULL DEFAULT 1,
    time_budget_ms       INTEGER      NOT NULL DEFAULT 0,
    created_at           DATETIME     NOT NULL DEFAULT NOW_UTC(),
    started_at           DATETIME     NOT NULL DEFAULT NOW_UTC(),
    updated_at           DATETIME     NOT NULL DEFAULT NOW_UTC()
);

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_flows_status ON dwarf_flows (status, updated_at);

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_flows_workflow_url ON dwarf_flows (workflow_url);

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_flows_surgraph ON dwarf_flows (surgraph_flow_id) WHERE surgraph_flow_id > 0;

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_flows_thread ON dwarf_flows (thread_id, flow_id);

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_flows_created_at ON dwarf_flows (created_at);
