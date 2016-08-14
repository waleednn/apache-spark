-- join nested table expressions (auto_join0.q)
SELECT a.k1, a.v1, a.k2, a.v2
FROM (
SELECT src1.key as k1, src1.value as v1,
       src2.key as k2, src2.value as v2 FROM
  (SELECT * FROM src WHERE src.key < 200) src1
    JOIN
  (SELECT * FROM src WHERE src.key < 200) src2
  SORT BY k1, v1, k2, v2
) a;

-- self-join + join condition (auto_join1.q)
SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key);

-- equi inner join + inner join with a complex join condition (auto_join2.q)
SELECT src1.key, src3.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key + src2.key = src3.key);

-- equi inner join + equi inner join (auto_join3.q)
SELECT src1.key, src3.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
 JOIN src src3 ON (src1.key = src3.key);

-- left-outer join over two nested table expressions (auto_join4.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
  SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
  FROM
  (
    SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
  ) a
  LEFT OUTER JOIN
  (
    SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
  ) b
  ON (a.c1 = b.c3)
) c;

-- right-outer join over two nested table expressions (auto_join5.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
  SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
  FROM
  (
    SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
  ) a
  RIGHT OUTER JOIN
  (
    SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
  ) b
  ON (a.c1 = b.c3)
) c;

-- full-outer join over two nested table expressions (auto_join6.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 FULL OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c;

-- full-outer join + left-outer join over nested table expressions (auto_join7.q)
SELECT c.c1, c.c2, c.c3, c.c4, c.c5, c.c6
FROM (
  SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4, c.c5 AS c5, c.c6 AS c6
  FROM
  (
    SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 10 and src1.key < 150
  ) a
  FULL OUTER JOIN
  (
    SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 150 and src2.key < 300
  ) b
  ON (a.c1 = b.c3)
  LEFT OUTER JOIN
  (
    SELECT src3.key AS c5, src3.value AS c6 FROM src src3 WHERE src3.key > 200 and src3.key < 400
  ) c
  ON (a.c1 = c.c5)
) c;

-- left-outer join + join condition + filter (auto_join8.q)
SELECT c.c1, c.c2, c.c3, c.c4
FROM (
 SELECT a.c1 AS c1, a.c2 AS c2, b.c3 AS c3, b.c4 AS c4
 FROM
 (
   SELECT src1.key AS c1, src1.value AS c2 FROM src src1 WHERE src1.key > 100 and src1.key < 300
 ) a
 LEFT OUTER JOIN
 (
   SELECT src2.key AS c3, src2.value AS c4 FROM src src2 WHERE src2.key > 200 and src2.key < 400
 ) b
 ON (a.c1 = b.c3)
) c
where c.c3 IS NULL AND c.c1 IS NOT NULL;

-- inner join + join condition + filter (auto_join9.q)
SELECT src1.key, src2.value
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.ds = '2008-04-08' and src1.hr = '12';

-- equi inner join + star expansion in nested table expression (auto_join10.q)
FROM
(SELECT src.* FROM src) x
JOIN
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
select Y.key, Y.value;

-- inner join with a complex join condition over nested table expressions (auto_join11.q)
SELECT src1.c1, src2.c4
FROM
  (SELECT src.key as c1, src.value as c2 from src) src1
  JOIN
  (SELECT src.key as c3, src.value as c4 from src) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 200;

-- two inner join with a complex join condition over nested table expressions (auto_join12.q)
SELECT src1.c1, src2.c4
FROM
  (SELECT src.key as c1, src.value as c2 from src) src1
  JOIN
  (SELECT src.key as c3, src.value as c4 from src) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 200
  JOIN
  (SELECT src.key as c5, src.value as c6 from src) src3
  ON src1.c1 = src3.c5 AND src3.c5 < 100;

-- two inner join with a complex join condition over nested table expressions (auto_join13.q)
SELECT src1.c1, src2.c4
FROM
  (SELECT src.key as c1, src.value as c2 from src) src1
  JOIN
  (SELECT src.key as c3, src.value as c4 from src) src2
  ON src1.c1 = src2.c3 AND src1.c1 < 250
  JOIN
  (SELECT src.key as c5, src.value as c6 from src) src3
  ON src1.c1 + src2.c3 = src3.c5 AND src3.c5 < 400;

-- join two different tables (auto_join14.q)
FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 200
SELECT src.key, srcpart.value;

-- join + sort by (auto_join15.q)
SELECT a.k1, a.v1, a.k2, a.v2
  FROM (
  SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2
  FROM src src1 JOIN src src2 ON (src1.key = src2.key)
  SORT BY k1, v1, k2, v2
  ) a;

-- inner join with a filter above join and a filter below join (auto_join16.q)
SELECT subq.key, tab.value
FROM
(select a.key, a.value from src a where a.key > 100 ) subq
JOIN src tab
ON (subq.key = tab.key and subq.key > 150 and subq.value = tab.value)
where tab.key < 200;

-- star expansion in nested table expression (auto_join17.q)
SELECT src1.*, src2.*
FROM src src1 JOIN src src2 ON (src1.key = src2.key);

-- full outer join over Aggregate (auto_join18.q)
SELECT a.key, a.value, b.key, b.value
FROM
  (
    SELECT src1.key as key, count(src1.value) AS value FROM src src1 group by src1.key
  ) a
  FULL OUTER JOIN
  (
    SELECT src2.key as key, count(distinct(src2.value)) AS value
    FROM src1 src2 group by src2.key
  ) b
ON (a.key = b.key);

-- full outer join + multi distinct (auto_join18_multi_distinct.q)
SELECT a.key, a.value, b.key, b.value1, b.value2
FROM
  (
    SELECT src1.key as key, count(src1.value) AS value FROM src src1 group by src1.key
  ) a
  FULL OUTER JOIN
  (
    SELECT src2.key as key, count(distinct(src2.value)) AS value1,
      count(distinct(src2.key)) AS value2
    FROM src1 src2 group by src2.key
  ) b
ON (a.key = b.key);

-- join + disjunctive conditions (auto_join19.q)
SELECT src1.key, src2.value
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
where (src1.ds = '2008-04-08' or src1.ds = '2008-04-09' )and (src1.hr = '12' or src1.hr = '11');

-- inner join + right-outer join #1 (auto_join20.q)
SELECT a.k1,a.v1,a.k2,a.v2,a.k3,a.v3
FROM (
  SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3, src3.value as v3
  FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 200)
    RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 300)
  SORT BY k1,v1,k2,v2,k3,v3
)a;

-- inner join + right-outer join #2 (auto_join20.q)
SELECT a.k1,a.v1,a.k2,a.v2,a.k3,a.v3
FROM (
  SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3, src3.value as v3
  FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key < 100)
    RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 300)
  SORT BY k1,v1,k2,v2,k3,v3
)a;

-- left outer join + right outer join (auto_join21.q)
SELECT *
FROM
  src src1
  LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
  RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- nested join (auto_join22.q)
SELECT src5.src1_value
FROM
  (SELECT src3.*, src4.value as src4_value, src4.key as src4_key
  FROM src src4
    JOIN (SELECT src2.*, src1.key as src1_key, src1.value as src1_value
      FROM src src1
        JOIN src src2 ON src1.key = src2.key) src3
    ON src3.src1_key = src4.key) src5;

-- Cartesian join (auto_join23.q)
SELECT  *  FROM src src1 JOIN src src2
WHERE src1.key < 200 and src2.key < 200
SORT BY src1.key, src1.value, src2.key, src2.value;

-- join (auto_join24.q)
WITH tst1 AS (SELECT a.key, count(1) as cnt FROM src a group by a.key)
SELECT sum(a.cnt) FROM tst1 a JOIN tst1 b ON a.key = b.key;

-- aggregate over join results (auto_join26.q)
SELECT x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key order by x.key;

-- join over set operation over aggregate (auto_join27.q)
SELECT count(1)
FROM
(
  SELECT src.key, src.value from src
  UNION ALL
  SELECT DISTINCT src.key, src.value from src
) src_12
JOIN
(
  SELECT src.key as k, src.value as v from src
) src3
ON src_12.key = src3.k AND src3.k < 300;

-- left outer + right outer (auto_join28.q)
SELECT * FROM src src1
 LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- right outer + right outer (auto_join28.q)
SELECT * FROM src src1
 RIGHT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- left outer + left outer (auto_join28.q)
SELECT * FROM src src1
 LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 LEFT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- right outer + left outer (auto_join28.q)
SELECT * FROM src src1
 RIGHT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 LEFT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- inner + left outer (auto_join29.q)
SELECT * FROM src src1
 JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 LEFT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- inner + right outer (auto_join29.q)
SELECT * FROM src src1
 JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 RIGHT OUTER JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- left + inner outer (auto_join29.q)
SELECT * FROM src src1
 LEFT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- right + inner join (auto_join29.q)
SELECT * FROM src src1
 RIGHT OUTER JOIN src src2 ON (src1.key = src2.key AND src1.key < 200 AND src2.key > 200)
 JOIN src src3 ON (src2.key = src3.key AND src3.key < 200)
 SORT BY src1.key, src1.value, src2.key, src2.value, src3.key, src3.value;

-- inner join with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select Y.key,Y.value;

-- left outer join with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select Y.key,Y.value;

-- right outer join with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select Y.key,Y.value;

-- inner + inner with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- inner + left outer with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- left + left outer with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- left + right outer with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- right + right outer with sorted by nested table expression (auto_join30.q)
FROM
(SELECT src.* FROM src sort by key) x
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;

-- right outer + inner with sorted by nested table expression (auto_join31.q)
FROM
(SELECT src.* FROM src sort by key) x
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select Y.key,Y.value;
