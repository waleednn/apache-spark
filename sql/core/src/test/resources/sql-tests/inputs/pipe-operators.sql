-- Prepare some test data.
--------------------------
drop table if exists t;
create table t(x int, y string) using csv;
insert into t values (0, 'abc'), (1, 'def');

drop table if exists other;
create table other(a int, b int) using json;
insert into other values (1, 1), (1, 2), (2, 4);

drop table if exists st;
create table st(x int, col struct<i1:int, i2:int>) using parquet;
insert into st values (1, (2, 3));

create temporary view courseSales as select * from values
  ("dotNET", 2012, 10000),
  ("Java", 2012, 20000),
  ("dotNET", 2012, 5000),
  ("dotNET", 2013, 48000),
  ("Java", 2013, 30000)
  as courseSales(course, year, earnings);

create temporary view years as select * from values
  (2012, 1),
  (2013, 2)
  as years(y, s);

create temporary view yearsWithComplexTypes as select * from values
  (2012, array(1, 1), map('1', 1), struct(1, 'a')),
  (2013, array(2, 2), map('2', 2), struct(2, 'b'))
  as yearsWithComplexTypes(y, a, m, s);

-- Selection operators: positive tests.
---------------------------------------

-- Selecting a constant.
table t
|> select 1 as x;

-- Selecting attributes.
table t
|> select x, y;

-- Chained pipe SELECT operators.
table t
|> select x, y
|> select x + length(y) as z;

-- Using the VALUES list as the source relation.
values (0), (1) tab(col)
|> select col * 2 as result;

-- Using a table subquery as the source relation.
(select * from t union all select * from t)
|> select x + length(y) as result;

-- Enclosing the result of a pipe SELECT operation in a table subquery.
(table t
 |> select x, y
 |> select x)
union all
select x from t where x < 1;

-- Selecting struct fields.
(select col from st)
|> select col.i1;

table st
|> select st.col.i1;

-- Expression subqueries in the pipe operator SELECT list.
table t
|> select (select a from other where x = a limit 1) as result;

-- Aggregations are allowed within expression subqueries in the pipe operator SELECT list as long as
-- no aggregate functions exist in the top-level select list.
table t
|> select (select any_value(a) from other where x = a limit 1) as result;

-- Lateral column aliases in the pipe operator SELECT list.
table t
|> select x + length(x) as z, z + 1 as plus_one;

-- Window functions are allowed in the pipe operator SELECT list.
table t
|> select first_value(x) over (partition by y) as result;

select 1 x, 2 y, 3 z
|> select 1 + sum(x) over (),
     avg(y) over (),
     x,
     avg(x+1) over (partition by y order by z) AS a2
|> select a2;

table t
|> select x, count(*) over ()
|> select x;

-- DISTINCT is supported.
table t
|> select distinct x, y;

-- Selection operators: negative tests.
---------------------------------------

-- Aggregate functions are not allowed in the pipe operator SELECT list.
table t
|> select sum(x) as result;

table t
|> select y, length(y) + sum(x) as result;

-- Pivot and unpivot operators: positive tests.
-----------------------------------------------

table courseSales
|> select `year`, course, earnings
|> pivot (
     sum(earnings)
     for course in ('dotNET', 'Java')
  );

table courseSales
|> select `year` as y, course as c, earnings as e
|> pivot (
     sum(e) as s, avg(e) as a
     for y in (2012 as firstYear, 2013 as secondYear)
   );

-- Pivot on multiple pivot columns with aggregate columns of complex data types.
(select course, `year`, y, a
 from courseSales
 join yearsWithComplexTypes on `year` = y)
|> pivot (
     max(a)
     for (y, course) in ((2012, 'dotNET'), (2013, 'Java'))
   );

-- Pivot on pivot column of struct type.
(select earnings, `year`, s
 from courseSales
 join yearsWithComplexTypes on `year` = y)
|> pivot (
     sum(earnings)
     for s in ((1, 'a'), (2, 'b'))
   );

-- Pivot and unpivot operators: negative tests.
-----------------------------------------------

-- The PIVOT operator refers to a column 'year' is not available in the input relation.
table courseSales
|> select course, earnings
|> pivot (
     sum(earnings)
     for `year` in (2012, 2013)
   );

-- Non-literal PIVOT values are not supported.
table courseSales
|> pivot (
     sum(earnings)
     for `year` in (course, 2013)
   );

-- Cleanup.
-----------
drop table t;
drop table other;
drop table st;
