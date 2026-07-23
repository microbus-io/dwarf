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
CREATE TABLE IF NOT EXISTS dwarf_peers (
    engine_id  BIGINT      NOT NULL,
    seen_at    DATETIME(3) NOT NULL DEFAULT NOW_UTC(),
    dispatches TINYINT     NOT NULL DEFAULT 1,
    PRIMARY KEY (engine_id)
);

-- DRIVER: pgx
CREATE TABLE IF NOT EXISTS dwarf_peers (
    engine_id  BIGINT      NOT NULL,
    seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW_UTC(),
    dispatches SMALLINT    NOT NULL DEFAULT 1,
    PRIMARY KEY (engine_id)
);

-- DRIVER: mssql
CREATE TABLE dwarf_peers (
    engine_id  BIGINT       NOT NULL,
    seen_at    DATETIME2(3) NOT NULL DEFAULT NOW_UTC(),
    dispatches TINYINT      NOT NULL DEFAULT 1,
    PRIMARY KEY (engine_id)
);

-- DRIVER: sqlite
CREATE TABLE IF NOT EXISTS dwarf_peers (
    engine_id  INTEGER  NOT NULL PRIMARY KEY,
    seen_at    DATETIME NOT NULL DEFAULT NOW_UTC(),
    dispatches INTEGER  NOT NULL DEFAULT 1
);
