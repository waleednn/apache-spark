-- Conversion of main primitive types to Byte type: 
SELECT CAST(NULL AS TINYINT) FROM src LIMIT 1;

SELECT CAST(TRUE AS TINYINT) FROM src LIMIT 1;

SELECT CAST(CAST(-18 AS SMALLINT) AS TINYINT) FROM src LIMIT 1;
SELECT CAST(-129 AS TINYINT) FROM src LIMIT 1;
SELECT CAST(CAST(-1025 AS BIGINT) AS TINYINT) FROM src LIMIT 1;

SELECT CAST(CAST(-3.14 AS DOUBLE) AS TINYINT) FROM src LIMIT 1;
SELECT CAST(CAST(-3.14 AS FLOAT) AS TINYINT) FROM src LIMIT 1;
SELECT CAST(CAST(-3.14 AS DECIMAL) AS TINYINT) FROM src LIMIT 1;

SELECT CAST('-38' AS TINYINT) FROM src LIMIT 1;

