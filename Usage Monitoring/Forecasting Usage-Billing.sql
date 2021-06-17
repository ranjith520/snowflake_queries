/* Description:
This query provides three distinct consumption metrics for each day of the contract term. (1) the contracted consumption is the dollar amount consumed if usage was flat for the entire term. (2) the actual consumption pulls from the various usage views and aggregates dollars at a day level. (3) the forecasted consumption creates a straight line regression from the actuals to project go-forward consumption.

How to Interpret Results:
This data should be mapped as line graphs with a running total calculation to estimate future forecast against the contract amount.

Primary Schema:
Account_Usage

SQL */
SET CREDIT_PRICE = 4.00; --edit this number to reflect credit price
SET TERM_LENGTH = 12; --integer value in months
SET TERM_START_DATE = '2020-01-01';
SET TERM_AMOUNT = 100000.00; --number(10,2) value in dollars
WITH CONTRACT_VALUES AS (
     SELECT
              $CREDIT_PRICE::decimal(10,2) as CREDIT_PRICE
             ,$TERM_AMOUNT::decimal(38,0) as TOTAL_CONTRACT_VALUE
             ,$TERM_START_DATE::timestamp as CONTRACT_START_DATE
             ,DATEADD(day,-1,DATEADD(month,$TERM_LENGTH,$TERM_START_DATE))::timestamp as CONTRACT_END_DATE
),
PROJECTED_USAGE AS (
     SELECT
                CREDIT_PRICE
               ,TOTAL_CONTRACT_VALUE
               ,CONTRACT_START_DATE
               ,CONTRACT_END_DATE
               ,(TOTAL_CONTRACT_VALUE)
                   /
                   DATEDIFF(day,CONTRACT_START_DATE,CONTRACT_END_DATE)  AS DOLLARS_PER_DAY
               , (TOTAL_CONTRACT_VALUE/CREDIT_PRICE)
                   /
               DATEDIFF(day,CONTRACT_START_DATE,CONTRACT_END_DATE) AS CREDITS_PER_DAY
     FROM      CONTRACT_VALUES
),
ACTUAL_USAGE AS (
 SELECT TO_DATE(START_TIME) AS CONSUMPTION_DATE
   ,SUM(DOLLARS_USED) as ACTUAL_DOLLARS_USED
 FROM (
   --COMPUTE FROM WAREHOUSES
   SELECT
            'WH Compute' as WAREHOUSE_GROUP_NAME
           ,WMH.WAREHOUSE_NAME
           ,NULL AS GROUP_CONTACT
           ,NULL AS GROUP_COST_CENTER
           ,NULL AS GROUP_COMMENT
           ,WMH.START_TIME
           ,WMH.END_TIME
           ,WMH.CREDITS_USED
           ,$CREDIT_PRICE
           ,($CREDIT_PRICE*WMH.CREDITS_USED) AS DOLLARS_USED
           ,'ACTUAL COMPUTE' AS MEASURE_TYPE                  
   from    SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WMH
   UNION ALL
   --COMPUTE FROM SNOWPIPE
   SELECT
            'Snowpipe' AS WAREHOUSE_GROUP_NAME
           ,PUH.PIPE_NAME AS WAREHOUSE_NAME
           ,NULL AS GROUP_CONTACT
           ,NULL AS GROUP_COST_CENTER
           ,NULL AS GROUP_COMMENT
           ,PUH.START_TIME
           ,PUH.END_TIME
           ,PUH.CREDITS_USED
           ,$CREDIT_PRICE
           ,($CREDIT_PRICE*PUH.CREDITS_USED) AS DOLLARS_USED
           ,'ACTUAL COMPUTE' AS MEASURE_TYPE
   from    SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY PUH
   UNION ALL
   --COMPUTE FROM CLUSTERING
   SELECT
            'Auto Clustering' AS WAREHOUSE_GROUP_NAME
           ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
           ,NULL AS GROUP_CONTACT
           ,NULL AS GROUP_COST_CENTER
           ,NULL AS GROUP_COMMENT
           ,ACH.START_TIME
           ,ACH.END_TIME
           ,ACH.CREDITS_USED
           ,$CREDIT_PRICE
           ,($CREDIT_PRICE*ACH.CREDITS_USED) AS DOLLARS_USED
           ,'ACTUAL COMPUTE' AS MEASURE_TYPE
   from    SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH
   UNION ALL
   --COMPUTE FROM MATERIALIZED VIEWS
   SELECT
            'Materialized Views' AS WAREHOUSE_GROUP_NAME
           ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
           ,NULL AS GROUP_CONTACT
           ,NULL AS GROUP_COST_CENTER
           ,NULL AS GROUP_COMMENT
           ,MVH.START_TIME
           ,MVH.END_TIME
           ,MVH.CREDITS_USED
           ,$CREDIT_PRICE
           ,($CREDIT_PRICE*MVH.CREDITS_USED) AS DOLLARS_USED
           ,'ACTUAL COMPUTE' AS MEASURE_TYPE
   from    SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY MVH
   UNION ALL
   --COMPUTE FROM SEARCH OPTIMIZATION
   SELECT
            'Search Optimization' AS WAREHOUSE_GROUP_NAME
           ,DATABASE_NAME || '.' || SCHEMA_NAME || '.' || TABLE_NAME AS WAREHOUSE_NAME
           ,NULL AS GROUP_CONTACT
           ,NULL AS GROUP_COST_CENTER
           ,NULL AS GROUP_COMMENT
           ,SOH.START_TIME
           ,SOH.END_TIME
           ,SOH.CREDITS_USED
           ,$CREDIT_PRICE
           ,($CREDIT_PRICE*SOH.CREDITS_USED) AS DOLLARS_USED
           ,'ACTUAL COMPUTE' AS MEASURE_TYPE
   from    SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY SOH
   UNION ALL
   --COMPUTE FROM REPLICATION
   SELECT
            'Replication' AS WAREHOUSE_GROUP_NAME
           ,DATABASE_NAME AS WAREHOUSE_NAME
           ,NULL AS GROUP_CONTACT
           ,NULL AS GROUP_COST_CENTER
           ,NULL AS GROUP_COMMENT
           ,RUH.START_TIME
           ,RUH.END_TIME
           ,RUH.CREDITS_USED
           ,$CREDIT_PRICE
           ,($CREDIT_PRICE*RUH.CREDITS_USED) AS DOLLARS_USED
           ,'ACTUAL COMPUTE' AS MEASURE_TYPE
   from    SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY RUH
   UNION ALL

   --STORAGE COSTS
   SELECT
            'Storage' AS WAREHOUSE_GROUP_NAME
           ,'Storage' AS WAREHOUSE_NAME
           ,NULL AS GROUP_CONTACT
           ,NULL AS GROUP_COST_CENTER
           ,NULL AS GROUP_COMMENT
           ,SU.USAGE_DATE
           ,SU.USAGE_DATE
           ,NULL AS CREDITS_USED
           ,$CREDIT_PRICE
           ,((STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES)/(1024*1024*1024*1024)*23)/DA.DAYS_IN_MONTH AS DOLLARS_USED
           ,'ACTUAL COMPUTE' AS MEASURE_TYPE
   from    SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE SU
   JOIN    (SELECT COUNT(*) AS DAYS_IN_MONTH,TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01') as DATE_MONTH FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM GROUP BY TO_DATE(DATE_PART('year',D_DATE)||'-'||DATE_PART('month',D_DATE)||'-01')) DA ON DA.DATE_MONTH = TO_DATE(DATE_PART('year',USAGE_DATE)||'-'||DATE_PART('month',USAGE_DATE)||'-01')
) A
 group by 1
),
FORECASTED_USAGE_SLOPE_INTERCEPT as (
 SELECT
          REGR_SLOPE(AU.ACTUAL_DOLLARS_USED,DATEDIFF(day,CONTRACT_START_DATE,AU.CONSUMPTION_DATE)) as SLOPE
          ,REGR_INTERCEPT(AU.ACTUAL_DOLLARS_USED,DATEDIFF(day,CONTRACT_START_DATE,AU.CONSUMPTION_DATE)) as INTERCEPT
 FROM        PROJECTED_USAGE PU
 JOIN        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM DA ON DA.D_DATE BETWEEN PU.CONTRACT_START_DATE AND PU.CONTRACT_END_DATE
 LEFT JOIN   ACTUAL_USAGE AU ON AU.CONSUMPTION_DATE = TO_DATE(DA.D_DATE)
)
SELECT
        DA.D_DATE::date as CONSUMPTION_DATE
       ,PU.DOLLARS_PER_DAY AS CONTRACTED_DOLLARS_USED
       ,AU.ACTUAL_DOLLARS_USED
       --the below is the mx+b equation to get the forecasted linear slope
       ,DATEDIFF(day,CONTRACT_START_DATE,DA.D_DATE)*FU.SLOPE + FU.INTERCEPT AS FORECASTED_DOLLARS_USED
FROM        PROJECTED_USAGE PU
JOIN        SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM    DA ON DA.D_DATE BETWEEN PU.CONTRACT_START_DATE AND PU.CONTRACT_END_DATE
LEFT JOIN   ACTUAL_USAGE                                    AU ON AU.CONSUMPTION_DATE = TO_DATE(DA.D_DATE)
JOIN        FORECASTED_USAGE_SLOPE_INTERCEPT                FU ON 1 = 1
;