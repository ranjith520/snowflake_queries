/* Description:
Full list of materialized views and the volume of credits consumed via the service over the last 30 days, broken out by day.

How to Interpret Results:
Look for irregularities in the credit consumption or consistently high consumption

Primary Schema:
Account_Usage

SQL */
SELECT 

TO_DATE(START_TIME) as DATE
,DATABASE_NAME
,SCHEMA_NAME
,TABLE_NAME
,SUM(CREDITS_USED) as CREDITS_USED

FROM "SNOWFLAKE"."ACCOUNT_USAGE"."MATERIALIZED_VIEW_REFRESH_HISTORY"

WHERE START_TIME >= dateadd(month,-1,current_timestamp()) 
GROUP BY 1,2,3,4
ORDER BY 5 DESC 
;