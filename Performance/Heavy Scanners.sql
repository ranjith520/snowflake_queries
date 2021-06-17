/* Description:Ordered list of users that run queries that scan a lot of data.

How to Interpret Results:This is a potential opportunity to train the user or enable clustering.

Primary Schema:Account_Usage

SQL */
select 
  User_name
, warehouse_name
, avg(case when partitions_total > 0 then partitions_scanned / partitions_total else 0 end) avg_pct_scanned
from   snowflake.account_usage.query_history
where  start_time::date > dateadd('days', -45, current_date)
group by 1, 2
order by 3 desc
;