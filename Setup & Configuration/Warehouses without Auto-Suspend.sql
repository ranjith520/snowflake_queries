/* Description:
Identifies all warehouses that do not have auto-suspend enabled. Enabling this feature will ensure that warehouses become suspended after a specific amount of inactive time in order to prevent runaway costs. By default, all warehouses have auto-suspend enabled.

How to Interpret Results:
Make sure all warehouses are set to auto suspend. This way when they are not processing queries your compute footprint will shrink and thus your credit burn.

SQL */
SHOW WAREHOUSES
;
SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
 WHERE IFNULL("auto_suspend",0) = 0
;
