-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT TRANSFORM (a, b, c, d) USING 'cat' FROM parquet_t2
--------------------------------------------------------------------------------
SELECT `gen_attr_4` AS `key`, `gen_attr_5` AS `value` FROM (SELECT TRANSFORM (`gen_attr_0`, `gen_attr_1`, `gen_attr_2`, `gen_attr_3`) USING 'cat' AS (`gen_attr_4` string, `gen_attr_5` string) FROM (SELECT `a` AS `gen_attr_0`, `b` AS `gen_attr_1`, `c` AS `gen_attr_2`, `d` AS `gen_attr_3` FROM `default`.`parquet_t2`) AS gen_subquery_0) AS gen_subquery_1
