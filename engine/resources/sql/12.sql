-- DRIVER: mysql
ALTER TABLE microbus_flows
    DROP COLUMN forked_flow_id,
    DROP COLUMN forked_step_depth;

-- DRIVER: pgx
ALTER TABLE microbus_flows
    DROP COLUMN forked_flow_id,
    DROP COLUMN forked_step_depth;

-- DRIVER: mssql
ALTER TABLE microbus_flows DROP COLUMN
    forked_flow_id,
    forked_step_depth;

-- DRIVER: sqlite
ALTER TABLE microbus_flows DROP COLUMN forked_flow_id;
-- DRIVER: sqlite
ALTER TABLE microbus_flows DROP COLUMN forked_step_depth;
