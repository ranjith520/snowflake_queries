/* Description:
Full list of pipes and the volume of credits consumed via the service over the last 30 days, broken out by day.

How to Interpret Results:
Look for irregularities in the credit consumption or consistently high consumption

Primary Schema:
Account_Usage

SQL */
SELECT 

TO_DATE(START_TIME) as DATE
,PIPE_NAME
,SUM(CREDITS_USED) as CREDITS_USED

FROM "SNOWFLAKE"."ACCOUNT_USAGE"."PIPE_USAGE_HISTORY"

WHERE START_TIME >= dateadd(month,-1,current_timestamp()) 
GROUP BY 1,2
ORDER BY 3 DESC 
;