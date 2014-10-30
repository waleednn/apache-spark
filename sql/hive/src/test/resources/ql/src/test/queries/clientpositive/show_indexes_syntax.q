set hive.stats.dbclass=fs;
DROP TABLE show_idx_t1;

CREATE TABLE show_idx_t1(KEY STRING, VALUE STRING);

CREATE INDEX idx_t1 ON TABLE show_idx_t1(KEY) AS "COMPACT" WITH DEFERRED REBUILD;
ALTER INDEX idx_t1 ON show_idx_t1 REBUILD;

EXPLAIN
SHOW INDEX ON show_idx_t1;

SHOW INDEX ON show_idx_t1;

EXPLAIN
SHOW INDEXES ON show_idx_t1;

SHOW INDEXES ON show_idx_t1;

EXPLAIN
SHOW FORMATTED INDEXES ON show_idx_t1;

SHOW FORMATTED INDEXES ON show_idx_t1;

DROP TABLE show_idx_t1;
