-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT TRANSFORM (key, value)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES('field.delim' = '|')
USING 'cat' AS (tKey, tValue)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES('field.delim' = '|')
FROM parquet_t1
--------------------------------------------------------------------------------
SELECT `gen_attr_2` AS `tKey`, `gen_attr_3` AS `tValue` FROM (SELECT TRANSFORM (`gen_attr_0`, `gen_attr_1`) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('field.delim' = '|') USING 'cat' AS (`gen_attr_2` string, `gen_attr_3` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES('field.delim' = '|') FROM (SELECT `key` AS `gen_attr_0`, `value` AS `gen_attr_1` FROM `default`.`parquet_t1`) AS gen_subquery_0) AS gen_subquery_1
