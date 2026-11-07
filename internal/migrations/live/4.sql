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


-- DRIVER: mysql mariadb
CREATE TABLE IF NOT EXISTS dwarf_flow_days (
    flow_id BIGINT NOT NULL,
    day     INT    NOT NULL,
    PRIMARY KEY (flow_id),
    INDEX idx_dwarf_flow_days_day (day)
);

-- DRIVER: pgx
CREATE TABLE IF NOT EXISTS dwarf_flow_days (
    flow_id BIGINT NOT NULL,
    day     INT    NOT NULL,
    PRIMARY KEY (flow_id)
);

-- DRIVER: pgx
CREATE INDEX IF NOT EXISTS idx_dwarf_flow_days_day ON dwarf_flow_days (day);

-- DRIVER: mssql
CREATE TABLE dwarf_flow_days (
    flow_id BIGINT NOT NULL,
    day     INT    NOT NULL,
    PRIMARY KEY (flow_id)
);

-- DRIVER: mssql
CREATE INDEX idx_dwarf_flow_days_day ON dwarf_flow_days (day);

-- DRIVER: sqlite
CREATE TABLE IF NOT EXISTS dwarf_flow_days (
    flow_id INTEGER NOT NULL PRIMARY KEY,
    day     INTEGER NOT NULL
);

-- DRIVER: sqlite
CREATE INDEX IF NOT EXISTS idx_dwarf_flow_days_day ON dwarf_flow_days (day);
