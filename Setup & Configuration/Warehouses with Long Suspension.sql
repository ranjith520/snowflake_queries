/* Description:
Identifies warehouses that have the longest setting for automatic suspension after a period of no activity on that warehouse.

How to Interpret Results:
All warehouses should have an appropriate setting for automatic suspension for the workload.

– For Tasks, Loading and ETL/ELT warehouses set to immediate suspension.

– For BI and SELECT query warehouses set to 10 minutes for suspension to keep data caches warm for end users

– For DevOps, DataOps and Data Science warehouses set to 5 minutes for suspension as warm cache is not as important to ad-hoc and highly unique queries.

SQL */
SHOW WAREHOUSES
;
SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
 WHERE "auto_suspend" >= 3600  // 3600 seconds = 1 hour
;