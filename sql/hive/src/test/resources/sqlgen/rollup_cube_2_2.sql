-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT key, value, count(value) FROM parquet_t1 GROUP BY key, value WITH CUBE
--------------------------------------------------------------------------------
SELECT `gen_attr_0` AS `key`, `gen_attr_1` AS `value`, `gen_attr_3` AS `count(value)` FROM (SELECT `gen_attr_5` AS `gen_attr_0`, `gen_attr_4` AS `gen_attr_1`, count(`gen_attr_4`) AS `gen_attr_3` FROM (SELECT `key` AS `gen_attr_5`, `value` AS `gen_attr_4` FROM `default`.`parquet_t1`) AS gen_subquery_0 GROUP BY `gen_attr_5`, `gen_attr_4` GROUPING SETS((`gen_attr_5`, `gen_attr_4`), (`gen_attr_5`), (`gen_attr_4`), ())) AS gen_subquery_1
