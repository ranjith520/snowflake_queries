/* Description:
Indicates whether the offset for the stream is positioned at a point earlier than the data retention period for the table (or 14 days, whichever period is longer). Change data capture (CDC) activity cannot be returned for the table.

How to Interpret Results:
To return CDC activity for the table, recreate the stream. To prevent a stream from becoming stale, consume the stream records within a transaction during the retention period for the table.

SQL */
SHOW STREAMS;

select * 
from table(result_scan(last_query_id())) 
where "stale" = true;