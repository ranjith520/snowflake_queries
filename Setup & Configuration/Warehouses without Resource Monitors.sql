/* Description:
Identifies all warehouses without resource monitors in place. Resource monitors provide the ability to set limits on credits consumed against a warehouse during a specific time interval or date range. This can help prevent certain warehouses from unintentionally consuming more credits than typically expected.

How to Interpret Results:
Warehouses without resource monitors in place could be prone to excessive costs if a warehouse consumes more credits than anticipated. Leverage the results of this query to identify the warehouses that should have resource monitors in place to prevent future runaway costs.

SQL */
SHOW WAREHOUSES
;
SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
 WHERE "resource_monitor" IS NULL
;