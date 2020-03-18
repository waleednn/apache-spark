-- date time functions

-- [SPARK-16836] current_date and current_timestamp literals
select current_date = current_date(), current_timestamp = current_timestamp();

select to_date(null), to_date('2016-12-31'), to_date('2016-12-31', 'yyyy-MM-dd');

select to_timestamp(null), to_timestamp('2016-12-31 00:12:00'), to_timestamp('2016-12-31', 'yyyy-MM-dd');

select dayofweek('2007-02-03'), dayofweek('2009-07-30'), dayofweek('2017-05-27'), dayofweek(null), dayofweek('1582-10-15 13:10:15');

-- [SPARK-22333]: timeFunctionCall has conflicts with columnReference
create temporary view ttf1 as select * from values
  (1, 2),
  (2, 3)
  as ttf1(current_date, current_timestamp);
  
select current_date, current_timestamp from ttf1;

create temporary view ttf2 as select * from values
  (1, 2),
  (2, 3)
  as ttf2(a, b);
  
select current_date = current_date(), current_timestamp = current_timestamp(), a, b from ttf2;

select a, b from ttf2 order by a, current_date;

select weekday('2007-02-03'), weekday('2009-07-30'), weekday('2017-05-27'), weekday(null), weekday('1582-10-15 13:10:15');

select year('1500-01-01'), month('1500-01-01'), dayOfYear('1500-01-01');


select date '2019-01-01\t';
select timestamp '2019-01-01\t';

-- time add/sub
select timestamp'2011-11-11 11:11:11' + interval '2' day;
select timestamp'2011-11-11 11:11:11' - interval '2' day;
select date'2011-11-11 11:11:11' + interval '2' second;
select date'2011-11-11 11:11:11' - interval '2' second;
select '2011-11-11' - interval '2' day;
select '2011-11-11 11:11:11' - interval '2' second;
select '1' - interval '2' second;
select 1 - interval '2' second;

-- subtract timestamps
select date'2020-01-01' - timestamp'2019-10-06 10:11:12.345678';
select timestamp'2019-10-06 10:11:12.345678' - date'2020-01-01';
select timestamp'2019-10-06 10:11:12.345678' - null;
select null - timestamp'2019-10-06 10:11:12.345678';

-- date add/sub
select date_add('2011-11-11', 1Y);
select date_add('2011-11-11', 1S);
select date_add('2011-11-11', 1);
select date_add('2011-11-11', 1L);
select date_add('2011-11-11', 1.0);
select date_add('2011-11-11', 1E1);
select date_add('2011-11-11', '1');
select date_add(date'2011-11-11', 1);
select date_add(timestamp'2011-11-11', 1);
select date_sub(date'2011-11-11', 1);
select date_sub(timestamp'2011-11-11', 1);
select date_sub(null, 1);
select date_sub(date'2011-11-11', null);
select date'2011-11-11' + 1E1;
select null + date '2001-09-28';
select date '2001-09-28' + 7Y;
select 7S + date '2001-09-28';
select date '2001-10-01' - 7;
select date '2001-09-28' + null;
select date '2001-09-28' - null;

-- subtract dates
select null - date '2019-10-06';
select date '2001-10-01' - date '2001-09-28';

-- variable-length tests
select to_timestamp('2019-10-06 10:11:12.', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.0', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.1', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.12', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.123UTC', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.1234', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.12345CST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.123456PST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
-- exceeded max variable length
select to_timestamp('2019-10-06 10:11:12.1234567PST', 'yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
-- special cases
select to_timestamp('123456 2019-10-06 10:11:12.123456PST', 'SSSSSS yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('223456 2019-10-06 10:11:12.123456PST', 'SSSSSS yyyy-MM-dd HH:mm:ss.SSSSSS[zzz]');
select to_timestamp('2019-10-06 10:11:12.1234', 'yyyy-MM-dd HH:mm:ss.[SSSSSS]');
select to_timestamp('2019-10-06 10:11:12.123', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]');
select to_timestamp('2019-10-06 10:11:12', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]');
select to_timestamp('2019-10-06 10:11:12.12', 'yyyy-MM-dd HH:mm[:ss.SSSSSS]');
select to_timestamp('2019-10-06 10:11', 'yyyy-MM-dd HH:mm[:ss.SSSSSS]');
select to_timestamp("2019-10-06S10:11:12.12345", "yyyy-MM-dd'S'HH:mm:ss.SSSSSS");
select to_timestamp("12.12342019-10-06S10:11", "ss.SSSSyyyy-MM-dd'S'HH:mm");
select to_timestamp("12.1232019-10-06S10:11", "ss.SSSSyyyy-MM-dd'S'HH:mm");
select to_timestamp("12.1232019-10-06S10:11", "ss.SSSSyy-MM-dd'S'HH:mm");
select to_timestamp("12.1234019-10-06S10:11", "ss.SSSSy-MM-dd'S'HH:mm");

select to_timestamp("2019-10-06S", "yyyy-MM-dd'S'");
select to_timestamp("S2019-10-06", "'S'yyyy-MM-dd");

select date_format(timestamp '2019-10-06', 'yyyy-MM-dd eeee');
select date_format(timestamp '2019-10-06', 'yyyy-MM-dd cccc');
select date_format(timestamp '2019-10-06', 'yyyy-MM-dd uuuu');
