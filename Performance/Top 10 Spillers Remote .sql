/* Description:
Identifies the top 10 worst offending queries in terms of bytes spilled to remote storage.

How to Interpret Results:
These queries should most likely be run on larger warehouses that have more local storage and memory.

Primary Schema:
Account_Usage

SQL */
select query_id, substr(query_text, 1, 50) partial_query_text, user_name, warehouse_name, warehouse_size, 
       BYTES_SPILLED_TO_REMOTE_STORAGE, start_time, end_time, total_elapsed_time/1000 total_elapsed_time
from   snowflake.account_usage.query_history
where  BYTES_SPILLED_TO_REMOTE_STORAGE > 0
and start_time::date > dateadd('days', -45, current_date)
order  by BYTES_SPILLED_TO_REMOTE_STORAGE desc
limit 10
;