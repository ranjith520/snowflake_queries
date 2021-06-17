/* Description:
Returns an ordered list of the longest running tasks

How to Interpret Results:
revisit task execution frequency or the task code for optimization

Primary Schema:
Account_Usage

SQL */
select DATEDIFF(seconds, QUERY_START_TIME,COMPLETED_TIME) as DURATION_SECONDS
                ,*
from snowflake.account_usage.task_history
WHERE STATE = 'SUCCEEDED'
and query_start_time >= DATEADD (day, -7, CURRENT_TIMESTAMP())
order by DURATION_SECONDS desc
  ;