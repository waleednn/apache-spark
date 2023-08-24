DROP VIEW IF EXISTS t1;
DROP VIEW IF EXISTS t2;
CREATE OR REPLACE TEMPORARY VIEW t1 AS VALUES (0, 1), (1, 2) t(c1, c2);
CREATE OR REPLACE TEMPORARY VIEW t2 AS VALUES (0, 1), (1, 2), (1, 3) t(partition_col, input);

-- test basic udtf
SELECT * FROM udtf(1, 2);
SELECT * FROM udtf(-1, 0);
SELECT * FROM udtf(0, -1);
SELECT * FROM udtf(0, 0);

-- test column alias
SELECT a, b FROM udtf(1, 2) t(a, b);

-- test lateral join
SELECT * FROM t1, LATERAL udtf(c1, c2);
SELECT * FROM t1 LEFT JOIN LATERAL udtf(c1, c2);
SELECT * FROM udtf(1, 2) t(c1, c2), LATERAL udtf(c1, c2);

-- test non-deterministic input
SELECT * FROM udtf(cast(rand(0) AS int) + 1, 1);

-- test UDTF calls that take input TABLE arguments
SELECT * FROM UDTFCountSumLast(TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFCountSumLast(TABLE(t2) PARTITION BY partition_col ORDER BY input);
SELECT * FROM UDTFCountSumLast(TABLE(t2) PARTITION BY partition_col ORDER BY input DESC);

-- test UDTF calls that take input TABLE arguments and the 'analyze' method returns required
-- partitioning and/or ordering properties for Catalyst to enforce for the input table
SELECT * FROM UDTFWithSinglePartition(TABLE(t2));
SELECT * FROM UDTFPartitionByOrderBy(TABLE(t2));
SELECT * FROM UDTFWithSinglePartition(TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFWithSinglePartition(TABLE(t2) PARTITION BY partition_col);
SELECT * FROM UDTFPartitionByOrderBy(TABLE(t2) WITH SINGLE PARTITION);
SELECT * FROM UDTFPartitionByOrderBy(TABLE(t2) PARTITION BY partition_col);

-- cleanup
DROP VIEW t1;
DROP VIEW t2;
