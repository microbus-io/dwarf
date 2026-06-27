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
CREATE TABLE IF NOT EXISTS dwarf_steps (
    step_id            BIGINT       NOT NULL AUTO_INCREMENT,
    flow_id            BIGINT       NOT NULL,
    step_depth         INT          NOT NULL,
    step_token         CHAR(16)     NOT NULL,
    task_name          VARCHAR(512) NOT NULL,
    task_url           VARCHAR(512) NOT NULL,
    state              JSON         NOT NULL DEFAULT ('{}'),
    changes            JSON         NOT NULL DEFAULT ('{}'),
    interrupt_payload  JSON         NOT NULL DEFAULT ('{}'),
    status             CHAR(16)     NOT NULL,
    goto_next          VARCHAR(512) NOT NULL DEFAULT '',
    error              TEXT         NOT NULL DEFAULT (''),
    time_budget_ms     INT          NOT NULL,
    attempt            INT          NOT NULL DEFAULT 0,
    lineage_id         BIGINT       NOT NULL DEFAULT 0,
    cohort_size        INT          NOT NULL DEFAULT 0,
    cohort_arrivals    INT          NOT NULL DEFAULT 0,
    cohort_failures    INT          NOT NULL DEFAULT 0,
    fan_out_ordinal    INT          NOT NULL DEFAULT 0,
    predecessor_id     BIGINT       NOT NULL DEFAULT 0,
    successor_id       BIGINT       NOT NULL DEFAULT 0,
    priority           INT          NOT NULL DEFAULT 5,
    fairness_key       VARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight    DOUBLE       NOT NULL DEFAULT 1,
    interrupt_done     TINYINT      NOT NULL DEFAULT 0,
    resume_data        JSON         NOT NULL DEFAULT ('{}'),
    subgraph_done      TINYINT      NOT NULL DEFAULT 0,
    subgraph_result    JSON         NOT NULL DEFAULT ('{}'),
    subgraph_error     TEXT         NOT NULL DEFAULT (''),
    parked             TINYINT      NOT NULL DEFAULT 0,
    not_before         DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    lease_expires      DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    created_at         DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    started_at         DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at         DATETIME(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (step_id),
    INDEX idx_dwarf_steps_flow_id (flow_id, step_id),
    INDEX idx_dwarf_steps_status (status, updated_at),
    INDEX idx_dwarf_steps_created_at (created_at),
    INDEX idx_dwarf_steps_selection (status, parked, priority, fairness_key),
    INDEX idx_dwarf_steps_saturation (status, parked, task_url)
);

-- DRIVER: pgx
CREATE TABLE IF NOT EXISTS dwarf_steps (
    step_id            BIGSERIAL    NOT NULL,
    flow_id            BIGINT       NOT NULL,
    step_depth         INT          NOT NULL,
    step_token         CHAR(16)     NOT NULL,
    task_name          VARCHAR(512) NOT NULL,
    task_url           VARCHAR(512) NOT NULL,
    state              JSONB        NOT NULL DEFAULT '{}',
    changes            JSONB        NOT NULL DEFAULT '{}',
    interrupt_payload  JSONB        NOT NULL DEFAULT '{}',
    status             CHAR(16)     NOT NULL,
    goto_next          VARCHAR(512) NOT NULL DEFAULT '',
    error              TEXT             NOT NULL DEFAULT '',
    time_budget_ms     INT          NOT NULL,
    attempt            INT          NOT NULL DEFAULT 0,
    lineage_id         BIGINT       NOT NULL DEFAULT 0,
    cohort_size        INT          NOT NULL DEFAULT 0,
    cohort_arrivals    INT          NOT NULL DEFAULT 0,
    cohort_failures    INT          NOT NULL DEFAULT 0,
    fan_out_ordinal    INT          NOT NULL DEFAULT 0,
    predecessor_id     BIGINT       NOT NULL DEFAULT 0,
    successor_id       BIGINT       NOT NULL DEFAULT 0,
    priority           INT          NOT NULL DEFAULT 5,
    fairness_key       VARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight    DOUBLE PRECISION NOT NULL DEFAULT 1,
    interrupt_done     SMALLINT     NOT NULL DEFAULT 0,
    resume_data        JSONB        NOT NULL DEFAULT '{}',
    subgraph_done      SMALLINT     NOT NULL DEFAULT 0,
    subgraph_result    JSONB        NOT NULL DEFAULT '{}',
    subgraph_error     TEXT             NOT NULL DEFAULT '',
    parked             SMALLINT     NOT NULL DEFAULT 0,
    not_before         TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    lease_expires      TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    started_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (step_id)
);

-- DRIVER: pgx
CREATE INDEX idx_dwarf_steps_flow_id ON dwarf_steps (flow_id);

-- DRIVER: pgx
CREATE INDEX idx_dwarf_steps_status ON dwarf_steps (status, updated_at) WHERE status IN ('pending', 'running');

-- DRIVER: pgx
CREATE INDEX idx_dwarf_steps_created_at ON dwarf_steps (created_at);

-- DRIVER: pgx
CREATE INDEX idx_dwarf_steps_selection ON dwarf_steps (status, parked, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: pgx
CREATE INDEX idx_dwarf_steps_saturation ON dwarf_steps (status, parked, task_url) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
CREATE TABLE dwarf_steps (
    step_id            BIGINT        NOT NULL IDENTITY(1,1),
    flow_id            BIGINT        NOT NULL,
    step_depth         INT           NOT NULL,
    step_token         NCHAR(16)     NOT NULL,
    task_name          NVARCHAR(512) NOT NULL,
    task_url           NVARCHAR(512) NOT NULL,
    state              NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    changes            NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    interrupt_payload  NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    status             NCHAR(16)     NOT NULL,
    goto_next          NVARCHAR(512) NOT NULL DEFAULT '',
    error              NVARCHAR(MAX) NOT NULL DEFAULT '',
    time_budget_ms     INT           NOT NULL,
    attempt            INT           NOT NULL DEFAULT 0,
    lineage_id         BIGINT        NOT NULL DEFAULT 0,
    cohort_size        INT           NOT NULL DEFAULT 0,
    cohort_arrivals    INT           NOT NULL DEFAULT 0,
    cohort_failures    INT           NOT NULL DEFAULT 0,
    fan_out_ordinal    INT           NOT NULL DEFAULT 0,
    predecessor_id     BIGINT        NOT NULL DEFAULT 0,
    successor_id       BIGINT        NOT NULL DEFAULT 0,
    priority           INT           NOT NULL DEFAULT 5,
    fairness_key       NVARCHAR(256) NOT NULL DEFAULT '',
    fairness_weight    FLOAT         NOT NULL DEFAULT 1,
    interrupt_done     TINYINT       NOT NULL DEFAULT 0,
    resume_data        NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    subgraph_done      TINYINT       NOT NULL DEFAULT 0,
    subgraph_result    NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    subgraph_error     NVARCHAR(MAX) NOT NULL DEFAULT '',
    parked             TINYINT       NOT NULL DEFAULT 0,
    not_before         DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    lease_expires      DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    created_at         DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    started_at         DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    updated_at         DATETIME2(3)  NOT NULL DEFAULT NOW_UTC(),
    PRIMARY KEY (step_id)
);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_steps_flow_id ON dwarf_steps (flow_id);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_steps_status ON dwarf_steps (status, updated_at) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
CREATE INDEX idx_dwarf_steps_created_at ON dwarf_steps (created_at);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_steps_selection ON dwarf_steps (status, parked, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: mssql
CREATE INDEX idx_dwarf_steps_saturation ON dwarf_steps (status, parked, task_url) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
CREATE TABLE IF NOT EXISTS dwarf_steps (
    step_id            INTEGER      NOT NULL PRIMARY KEY AUTOINCREMENT,
    flow_id            INTEGER      NOT NULL,
    step_depth         INTEGER      NOT NULL,
    step_token         TEXT         NOT NULL,
    task_name          TEXT         NOT NULL,
    task_url           TEXT         NOT NULL,
    state              TEXT         NOT NULL DEFAULT '{}',
    changes            TEXT         NOT NULL DEFAULT '{}',
    interrupt_payload  TEXT         NOT NULL DEFAULT '{}',
    status             TEXT         NOT NULL,
    goto_next          TEXT         NOT NULL DEFAULT '',
    error              TEXT         NOT NULL DEFAULT '',
    time_budget_ms     INTEGER      NOT NULL,
    attempt            INTEGER      NOT NULL DEFAULT 0,
    lineage_id         INTEGER      NOT NULL DEFAULT 0,
    cohort_size        INTEGER      NOT NULL DEFAULT 0,
    cohort_arrivals    INTEGER      NOT NULL DEFAULT 0,
    cohort_failures    INTEGER      NOT NULL DEFAULT 0,
    fan_out_ordinal    INTEGER      NOT NULL DEFAULT 0,
    predecessor_id     INTEGER      NOT NULL DEFAULT 0,
    successor_id       INTEGER      NOT NULL DEFAULT 0,
    priority           INTEGER      NOT NULL DEFAULT 5,
    fairness_key       TEXT         NOT NULL DEFAULT '',
    fairness_weight    REAL         NOT NULL DEFAULT 1,
    interrupt_done     INTEGER      NOT NULL DEFAULT 0,
    resume_data        TEXT         NOT NULL DEFAULT '{}',
    subgraph_done      INTEGER      NOT NULL DEFAULT 0,
    subgraph_result    TEXT         NOT NULL DEFAULT '{}',
    subgraph_error     TEXT         NOT NULL DEFAULT '',
    parked             INTEGER      NOT NULL DEFAULT 0,
    not_before         DATETIME     NOT NULL DEFAULT NOW_UTC(),
    lease_expires      DATETIME     NOT NULL DEFAULT NOW_UTC(),
    created_at         DATETIME     NOT NULL DEFAULT NOW_UTC(),
    started_at         DATETIME     NOT NULL DEFAULT NOW_UTC(),
    updated_at         DATETIME     NOT NULL DEFAULT NOW_UTC()
);

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_steps_flow_id ON dwarf_steps (flow_id, step_id);

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_steps_status ON dwarf_steps (status, updated_at) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_steps_created_at ON dwarf_steps (created_at);

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_steps_selection ON dwarf_steps (status, parked, priority, fairness_key) WHERE status IN ('pending', 'running');

-- DRIVER: sqlite
CREATE INDEX idx_dwarf_steps_saturation ON dwarf_steps (status, parked, task_url) WHERE status IN ('pending', 'running');
