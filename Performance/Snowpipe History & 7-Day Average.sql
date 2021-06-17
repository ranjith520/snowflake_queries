/* Description:
Average daily credits consumed by Snowpipe grouped by week over the last year.

How to Interpret Results:
Look for anomalies in the daily average over the course of the year. Opportunity to investigate the spikes or changes in consumption.

Primary Schema:
Account_Usage

SQL */
WITH CREDITS_BY_DAY AS (
SELECT TO_DATE(START_TIME) as DATE
,SUM(CREDITS_USED) as CREDITS_USED


FROM "SNOWFLAKE"."ACCOUNT_USAGE"."PIPE_USAGE_HISTORY"

WHERE START_TIME >= dateadd(year,-1,current_timestamp()) 
GROUP BY 1
ORDER BY 2 DESC 
  )
  
SELECT DATE_TRUNC('week',DATE)
,AVG(CREDITS_USED) as AVG_DAILY_CREDITS
FROM CREDITS_BY_DAY
GROUP BY 1
ORDER BY 1
;