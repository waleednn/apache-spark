-- Test aggregate operator and UDAF with codegen on and off.
--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
(1), (2), (3), (4)
as t1(int_col1);

CREATE FUNCTION myDoubleAvg AS 'test.org.apache.spark.sql.MyDoubleAvg';

SELECT default.myDoubleAvg(int_col1) as my_avg from t1;

SELECT default.myDoubleAvg(int_col1, 3) as my_avg from t1;

CREATE FUNCTION udaf1 AS 'test.non.existent.udaf';

SELECT default.udaf1(int_col1) as udaf1 from t1;

CREATE FUNCTION myDoubleAverage AS 'org.apache.spark.sql.MyDoubleAverage';

SELECT default.myDoubleAverage(int_col1) as my_avg from t1;

SELECT default.myDoubleAverage(int_col1, 3) as my_avg from t1;

CREATE FUNCTION myDoubleAverage2 AS 'test.org.apache.spark.sql.MyDoubleAverage';

SELECT default.myDoubleAverage2(int_col1) as my_avg from t1;

CREATE FUNCTION MyDoubleSum AS 'org.apache.spark.sql.MyDoubleSum';

SELECT default.MyDoubleSum(int_col1) as my_sum from t1;

DROP FUNCTION myDoubleAvg;
DROP FUNCTION udaf1;
DROP FUNCTION myDoubleAverage;
DROP FUNCTION myDoubleAverage2;
DROP FUNCTION MyDoubleSum;