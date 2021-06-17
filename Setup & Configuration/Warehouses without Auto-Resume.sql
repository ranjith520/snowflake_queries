/* Description:
Identifies all warehouses that do not have auto-resume enabled. Enabling this feature will automatically resume a warehouse any time a query is submitted against that specific warehouse. By default, all warehouses have auto-resume enabled.

How to Interpret Results:
Make sure all warehouses are set to auto resume. If you are going to implement auto suspend and proper timeout limits, this is a must or users will not be able to query the system.

SQL */
SHOW WAREHOUSES
;
SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
 WHERE "auto_resume" = 'false'
;