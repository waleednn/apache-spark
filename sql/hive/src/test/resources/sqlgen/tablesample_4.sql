-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT * FROM t0 TABLESAMPLE(100 PERCENT)
--------------------------------------------------------------------------------
SELECT t0.`id` FROM `default`.`t0` TABLESAMPLE(100.0 PERCENT)
