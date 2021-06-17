/* Description:
This query returns the daily average of credit consumption grouped by week and warehouse.

How to Interpret Results:
Use this to identify anomolies in credit consumption for warehouses across weeks from the past year.

Primary Schema:
Account_Usage

SQL */
WITH CTE_DATE_WH AS(
  SELECT TO_DATE(START_TIME) AS START_DATE
        ,WAREHOUSE_NAME
        ,SUM(CREDITS_USED) AS CREDITS_USED_DATE_WH
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
   GROUP BY START_DATE
           ,WAREHOUSE_NAME
)
SELECT START_DATE
      ,WAREHOUSE_NAME
      ,CREDITS_USED_DATE_WH
      ,AVG(CREDITS_USED_DATE_WH) OVER (PARTITION BY WAREHOUSE_NAME ORDER BY START_DATE ROWS 7 PRECEDING) AS CREDITS_USED_7_DAY_AVG
      ,100.0*((CREDITS_USED_DATE_WH / CREDITS_USED_7_DAY_AVG) - 1) AS PCT_OVER_TO_7_DAY_AVERAGE
  FROM CTE_DATE_WH
QUALIFY CREDITS_USED_DATE_WH > 100  // Minimum N=100 credits
    AND PCT_OVER_TO_7_DAY_AVERAGE >= 0.5  // Minimum 50% increase over past 7 day average
 ORDER BY PCT_OVER_TO_7_DAY_AVERAGE DESC
;