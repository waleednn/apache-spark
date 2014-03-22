EXPLAIN
 SELECT a.key, a.value, b.key, b.value1,  b.value2
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
 ON (a.key = b.key) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value1 ASC, b.value2 ASC;

 SELECT a.key, a.value, b.key, b.value1,  b.value2
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
 ON (a.key = b.key) ORDER BY a.key ASC, a.value ASC, b.key ASC, b.value1 ASC, b.value2 ASC;
