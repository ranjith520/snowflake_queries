/* Description:
Shows the total credit consumption on an hourly basis to help understand consumption trends (peaks, valleys) over the past 7 days.

How to Interpret Results:
At which points of the day are we seeing spikes in our consumption? Is that expected?

Primary Schema:
Account_Usage

SQL (by hour, warehouse) */
// Credits used by [hour, warehouse] (past 7 days)
SELECT START_TIME
      ,WAREHOUSE_NAME
      ,CREDITS_USED_COMPUTE
  FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
 WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
   AND WAREHOUSE_ID > 0  // Skip pseudo-VWs such as "CLOUD_SERVICES_ONLY"
 ORDER BY 1 DESC,2
;