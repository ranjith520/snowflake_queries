/* Description:
This query is designed to give a rough idea of how busy Warehouses are compared to the credit consumption per hour. It will show the end user the number of credits consumed, the number of queries executed and the total execution time of those queries in each hour window.

How to Interpret Results:
This data can be used to draw correlations between credit consumption and the #/duration of query executions. The more queries or higher query duration for the fewest number of credits may help drive more value per credit.

Primary Schema:
Account_Usage

SQL */
SELECT
       WMH.WAREHOUSE_NAME
      ,WMH.START_TIME
      ,WMH.CREDITS_USED
      ,SUM(COALESCE(B.EXECUTION_TIME_SECONDS,0)) as TOTAL_EXECUTION_TIME_SECONDS
      ,SUM(COALESCE(QUERY_COUNT,0)) AS QUERY_COUNT

FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
LEFT JOIN (

      --QUERIES FULLY EXECUTED WITHIN THE HOUR
      SELECT
         WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,SUM(COALESCE(QH.EXECUTION_TIME,0))/(1000) AS EXECUTION_TIME_SECONDS
        ,COUNT(DISTINCT QH.QUERY_ID) AS QUERY_COUNT
      FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY     WMH
      JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY             QH ON QH.WAREHOUSE_NAME = WMH.WAREHOUSE_NAME
                                                                          AND QH.START_TIME BETWEEN WMH.START_TIME AND WMH.END_TIME
                                                                          AND QH.END_TIME BETWEEN WMH.START_TIME AND WMH.END_TIME
      WHERE TO_DATE(WMH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      AND TO_DATE(QH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      GROUP BY
      WMH.WAREHOUSE_NAME
      ,WMH.START_TIME

      UNION ALL

      --FRONT part OF QUERIES Executed longer than 1 Hour
      SELECT
         WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,SUM(COALESCE(DATEDIFF(seconds,QH.START_TIME,WMH.END_TIME),0)) AS EXECUTION_TIME_SECONDS
        ,COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT
      FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY     WMH
      JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY             QH ON QH.WAREHOUSE_NAME = WMH.WAREHOUSE_NAME
                                                                          AND QH.START_TIME BETWEEN WMH.START_TIME AND WMH.END_TIME
                                                                          AND QH.END_TIME > WMH.END_TIME
      WHERE TO_DATE(WMH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      AND TO_DATE(QH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      GROUP BY
      WMH.WAREHOUSE_NAME
      ,WMH.START_TIME

      UNION ALL

      --Back part OF QUERIES Executed longer than 1 Hour
      SELECT
         WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,SUM(COALESCE(DATEDIFF(seconds,WMH.START_TIME,QH.END_TIME),0)) AS EXECUTION_TIME_SECONDS
        ,COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT
      FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY     WMH
      JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY             QH ON QH.WAREHOUSE_NAME = WMH.WAREHOUSE_NAME
                                                                          AND QH.END_TIME BETWEEN WMH.START_TIME AND WMH.END_TIME
                                                                          AND QH.START_TIME < WMH.START_TIME
      WHERE TO_DATE(WMH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      AND TO_DATE(QH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      GROUP BY
      WMH.WAREHOUSE_NAME
      ,WMH.START_TIME

      UNION ALL

      --Middle part OF QUERIES Executed longer than 1 Hour
      SELECT
         WMH.WAREHOUSE_NAME
        ,WMH.START_TIME
        ,SUM(COALESCE(DATEDIFF(seconds,WMH.START_TIME,WMH.END_TIME),0)) AS EXECUTION_TIME_SECONDS
        ,COUNT(DISTINCT QUERY_ID) AS QUERY_COUNT
      FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY     WMH
      JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY             QH ON QH.WAREHOUSE_NAME = WMH.WAREHOUSE_NAME
                                                                          AND WMH.START_TIME > QH.START_TIME
                                                                          AND WMH.END_TIME < QH.END_TIME
      WHERE TO_DATE(WMH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      AND TO_DATE(QH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
      GROUP BY
      WMH.WAREHOUSE_NAME
      ,WMH.START_TIME

) B ON B.WAREHOUSE_NAME = WMH.WAREHOUSE_NAME AND B.START_TIME = WMH.START_TIME

WHERE TO_DATE(WMH.START_TIME) >= DATEADD(week,-1,CURRENT_TIMESTAMP())
GROUP BY

      WMH.WAREHOUSE_NAME
      ,WMH.START_TIME
      ,WMH.CREDITS_USED
;