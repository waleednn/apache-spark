-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT COUNT(value) FROM parquet_t1 GROUP BY key ORDER BY MAX(key)
--------------------------------------------------------------------------------
SELECT `count(value)` FROM (SELECT count(parquet_t1.`value`) AS `count(value)`, max(parquet_t1.`key`) AS `aggOrder0` FROM parquet_t1 GROUP BY parquet_t1.`key` ORDER BY `aggOrder0` ASC)
