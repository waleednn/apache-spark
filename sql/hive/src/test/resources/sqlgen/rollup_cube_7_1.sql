-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT a, b, grouping_id(a, b) FROM parquet_t2 GROUP BY cube(a, b)
--------------------------------------------------------------------------------
SELECT parquet_t2.`a` AS `a`, parquet_t2.`b` AS `b`, grouping_id() AS `grouping_id(a, b)` FROM (SELECT parquet_t2.`a`, parquet_t2.`b`, parquet_t2.`c`, parquet_t2.`d`, parquet_t2.`a` AS `a`, parquet_t2.`b` AS `b` FROM parquet_t2) GROUP BY parquet_t2.`a`, parquet_t2.`b` GROUPING SETS((parquet_t2.`a`, parquet_t2.`b`), (parquet_t2.`a`), (parquet_t2.`b`), ())
