/* Description:
Shows average number of queries run on an hourly basis to help better understand typical query activity.

How to Interpret Results:
How many queries are being run on an hourly basis? Is this more or less than we anticipated? What could be causing this?

Primary Schema:
Account_Usage

SQL */
SELECT DATE_TRUNC('HOUR', START_TIME) AS QUERY_START_HOUR
      ,WAREHOUSE_NAME
      ,COUNT(*) AS NUM_QUERIES
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
 WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())  // Past 7 days
 GROUP BY 1, 2
 ORDER BY 1 DESC, 2
;