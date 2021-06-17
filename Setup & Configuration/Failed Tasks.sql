/* Description:
Returns a list of task executions that failed.

How to Interpret Results:
Revisit these task executions to resolve the errors.

Primary Schema:
Account_Usage

SQL */
select *
  from snowflake.account_usage.task_history
  WHERE STATE = 'FAILED'
  and query_start_time >= DATEADD (day, -7, CURRENT_TIMESTAMP())
  order by query_start_time DESC
  ;