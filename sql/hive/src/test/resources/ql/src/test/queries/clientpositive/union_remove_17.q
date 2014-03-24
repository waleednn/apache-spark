set hive.stats.autogather=false;
set hive.optimize.union.remove=true;
set hive.mapred.supports.subdirectories=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set mapred.input.dir.recursive=true;

-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select star and a file sink
-- and the results are written to a table using dynamic partitions.
-- There is no need for this optimization, since the query is a map-only query.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1, it might be easier
-- to run the test only on hadoop 23

create table inputTbl1(key string, val string) stored as textfile;
create table outputTbl1(key string, values bigint) partitioned by (ds string) stored as rcfile;

load data local inpath '../data/files/T1.txt' into table inputTbl1;

explain
insert overwrite table outputTbl1 partition (ds)
SELECT *
FROM (
  SELECT key, 1 as values, '1' as ds from inputTbl1
  UNION ALL
  SELECT key, 2 as values, '2' as ds from inputTbl1
) a;

insert overwrite table outputTbl1 partition (ds)
SELECT *
FROM (
  SELECT key, 1 as values, '1' as ds from inputTbl1
  UNION ALL
  SELECT key, 2 as values, '2' as ds from inputTbl1
) a;

desc formatted outputTbl1;
show partitions outputTbl1;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1 where ds = '1' order by key, values;
select * from outputTbl1 where ds = '2' order by key, values;
