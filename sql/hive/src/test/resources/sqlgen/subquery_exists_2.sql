-- This file is automatically generated by LogicalPlanToSQLSuite.
select *
from (select *
      from src b
      where exists (select a.key
                    from src a
                    where b.value = a.value and a.key = b.key and a.value > 'val_9')) a
--------------------------------------------------------------------------------
SELECT a.`key`, a.`value` FROM (SELECT b.`key`, b.`value` FROM (`default`.`src`) AS b WHERE EXISTS (SELECT a.`key`, a.`value` FROM (`default`.`src`) AS a WHERE (a.`value` > 'val_9') AND (b.`value` = a.`value`) AND (a.`key` = b.`key`))) AS a
