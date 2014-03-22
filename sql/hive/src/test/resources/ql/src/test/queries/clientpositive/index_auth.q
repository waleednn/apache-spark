SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
create table foobar(key int, value string) PARTITIONED BY (ds string, hr string);
alter table foobar add partition (ds='2008-04-08',hr='12');

CREATE INDEX srcpart_auth_index ON TABLE foobar(key) as 'BITMAP' WITH DEFERRED REBUILD;
grant select on table foobar to user hive_test_user;
grant select on table default__foobar_srcpart_auth_index__ to user hive_test_user;
grant update on table default__foobar_srcpart_auth_index__ to user hive_test_user;
grant create on table default__foobar_srcpart_auth_index__ to user hive_test_user;
set hive.security.authorization.enabled=true;

ALTER INDEX srcpart_auth_index ON foobar PARTITION (ds='2008-04-08',hr='12')  REBUILD;
set hive.security.authorization.enabled=false;
DROP INDEX srcpart_auth_index on foobar;
DROP TABLE foobar;
