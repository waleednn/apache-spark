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

create temporary view yearsWithArray as select * from values
  (2012, array(1, 1)),
  (2013, array(2, 2))
  as yearsWithArray(y, a);

-- pivot courses
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(earnings)
  FOR course IN ('dotNET', 'Java')
);

-- pivot years with no subquery
SELECT * FROM courseSales
PIVOT (
  sum(earnings)
  FOR year IN (2012, 2013)
);

-- pivot courses with multiple aggregations
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(earnings), avg(earnings)
  FOR course IN ('dotNET', 'Java')
);

-- pivot with no group by column
SELECT * FROM (
  SELECT course, earnings FROM courseSales
)
PIVOT (
  sum(earnings)
  FOR course IN ('dotNET', 'Java')
);

-- pivot with no group by column and with multiple aggregations on different columns
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(earnings), min(year)
  FOR course IN ('dotNET', 'Java')
);

-- pivot on join query with multiple group by columns
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings)
  FOR s IN (1, 2)
);

-- pivot on join query with multiple aggregations on different columns
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings), min(s)
  FOR course IN ('dotNET', 'Java')
);

-- pivot on join query with multiple columns in one aggregation
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings * s)
  FOR course IN ('dotNET', 'Java')
);

-- pivot with aliases and projection
SELECT 2012_s, 2013_s, 2012_a, 2013_a, c FROM (
  SELECT year y, course c, earnings e FROM courseSales
)
PIVOT (
  sum(e) s, avg(e) a
  FOR y IN (2012, 2013)
);

-- pivot with projection and value aliases
SELECT firstYear_s, secondYear_s, firstYear_a, secondYear_a, c FROM (
  SELECT year y, course c, earnings e FROM courseSales
)
PIVOT (
  sum(e) s, avg(e) a
  FOR y IN (2012 as firstYear, 2013 secondYear)
);

-- pivot years with non-aggregate function
SELECT * FROM courseSales
PIVOT (
  abs(earnings)
  FOR year IN (2012, 2013)
);

-- pivot with one of the expressions as non-aggregate function
SELECT * FROM (
  SELECT year, course, earnings FROM courseSales
)
PIVOT (
  sum(earnings), year
  FOR course IN ('dotNET', 'Java')
);

-- pivot with unresolvable columns
SELECT * FROM (
  SELECT course, earnings FROM courseSales
)
PIVOT (
  sum(earnings)
  FOR year IN (2012, 2013)
);

-- pivot on multiple pivot columns
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings)
  FOR (course, year) IN (('dotNET', 2012), ('Java', 2013))
);

-- pivot on multiple pivot columns with aliased values
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings)
  FOR (course, s) IN (('dotNET', 2) as c1, ('Java', 1) as c2)
);

-- pivot on multiple pivot columns with values of wrong data types
SELECT * FROM (
  SELECT course, year, earnings, s
  FROM courseSales
  JOIN years ON year = y
)
PIVOT (
  sum(earnings)
  FOR (course, year) IN ('dotNET', 'Java')
);

-- pivot with unresolvable values
SELECT * FROM courseSales
PIVOT (
  sum(earnings)
  FOR year IN (s, 2013)
);

-- pivot with non-literal values
SELECT * FROM courseSales
PIVOT (
  sum(earnings)
  FOR year IN (course, 2013)
);

-- pivot on join query with columns of complex data types
SELECT * FROM (
  SELECT course, year, a
  FROM courseSales
  JOIN yearsWithArray ON year = y
)
PIVOT (
  min(a)
  FOR course IN ('dotNET', 'Java')
);

-- pivot on multiple pivot columns with agg columns of complex data types
SELECT * FROM (
  SELECT course, year, y, a
  FROM courseSales
  JOIN yearsWithArray ON year = y
)
PIVOT (
  max(a)
  FOR (y, course) IN ((2012, 'dotNET'), (2013, 'Java'))
);
