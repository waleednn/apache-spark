-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT a, b, sum(c) FROM parquet_t2 GROUP BY a, b GROUPING SETS (a) ORDER BY a, b
--------------------------------------------------------------------------------
SELECT parquet_t2.`a` AS `a`, parquet_t2.`b` AS `b`, sum(parquet_t2.`c`) AS `sum(c)` FROM (SELECT parquet_t2.`a`, parquet_t2.`b`, parquet_t2.`c`, parquet_t2.`d`, parquet_t2.`a` AS `a`, parquet_t2.`b` AS `b` FROM parquet_t2) GROUP BY parquet_t2.`a`, parquet_t2.`b` GROUPING SETS((parquet_t2.`a`)) ORDER BY `a` ASC, `b` ASC
