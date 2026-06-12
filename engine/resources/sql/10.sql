-- DRIVER: mysql
ALTER TABLE microbus_steps
    ADD COLUMN interrupt_done  TINYINT NOT NULL DEFAULT 0,
    ADD COLUMN resume_data     JSON    NOT NULL DEFAULT ('{}'),
    ADD COLUMN subgraph_done   TINYINT NOT NULL DEFAULT 0,
    ADD COLUMN subgraph_result JSON    NOT NULL DEFAULT ('{}'),
    ADD COLUMN subgraph_error  TEXT    NOT NULL DEFAULT ('');

-- DRIVER: pgx
ALTER TABLE microbus_steps
    ADD COLUMN interrupt_done  SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN resume_data     JSONB    NOT NULL DEFAULT '{}',
    ADD COLUMN subgraph_done   SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN subgraph_result JSONB    NOT NULL DEFAULT '{}',
    ADD COLUMN subgraph_error  TEXT     NOT NULL DEFAULT '';

-- DRIVER: mssql
ALTER TABLE microbus_steps ADD
    interrupt_done  TINYINT       NOT NULL DEFAULT 0,
    resume_data     NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    subgraph_done   TINYINT       NOT NULL DEFAULT 0,
    subgraph_result NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    subgraph_error  NVARCHAR(MAX) NOT NULL DEFAULT '';

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN interrupt_done INTEGER NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN resume_data TEXT NOT NULL DEFAULT '{}';

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN subgraph_done INTEGER NOT NULL DEFAULT 0;

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN subgraph_result TEXT NOT NULL DEFAULT '{}';

-- DRIVER: sqlite
ALTER TABLE microbus_steps ADD COLUMN subgraph_error TEXT NOT NULL DEFAULT '';
