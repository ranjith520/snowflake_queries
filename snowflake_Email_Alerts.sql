
use Role accountadmin;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT AUDIT ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT MANAGE ORGANIZATION SUPPORT CASES ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT APPLY TAG ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT MONITOR USAGE ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT APPLY PASSWORD POLICY ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT APPLY SESSION POLICY ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT MONITOR ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT MANAGE ACCOUNT SUPPORT CASES ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT ATTACH POLICY ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE ROLE_SF_DATAARCHITECT;
grant execute alert on account to role ROLE_SF_DATAARCHITECT;

------------------------------------------------------------- ALERTS---------CREATION---------------------------------------------------------------

use role ROLE_SF_DATAARCHITECT;

set DB_Name = 'EDWDEV';
set DB_Schema = 'ADMIN';
set Warehouse_Name = 'WH_DATAARCHITECT';

use database IDENTIFIER($DB_Name);

CREATE OR REPLACE SCHEMA IDENTIFIER($DB_Schema) WITH MANAGED ACCESS ;

use schema IDENTIFIER($DB_Schema);

create or replace notification integration NOTIFICATION_INT_ALL_ANALYTICS_TEAMS
    type=email
    enabled=true
    allowed_recipients=('username1@company.com','emailuser1@company.com','emailuser12@company.com','emailuser13@company.com');

    'emailuser14@company.com',



----------------------------Creating a Stored procedure for Emailing results based on sql query records > 1  -----------------------

use database IDENTIFIER($DB_Name);
use schema IDENTIFIER($DB_Schema);

CREATE OR REPLACE PROCEDURE SP_EMAIL_SQLQUERY_RESULTS(send_to STRING, subject STRING, sqlquery STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'x'
EXECUTE AS CALLER
AS
$$
import snowflake
import pandas as pd

def x(session, send_to, subject, sqlquery):
    try:
        # Get Database and Account names
        database_name = session.sql("SELECT CURRENT_DATABASE()").to_pandas().iloc[0,0]
        account_name = session.sql("SELECT CURRENT_ACCOUNT()").to_pandas().iloc[0,0]

        # Main query
        query = sqlquery
        body = session.sql(query).to_pandas()

        if len(body) > 0:
            # Convert the DataFrame to an HTML table using pandas
            Body_Table = body.to_html(index=False, classes='custom_table')

            # Add CSS to customize the table's appearance
            custom_css = """
            <style>
                .custom_table {
                    border-collapse: collapse;
                    width: 100%;
                }
                .custom_table th {
                    background-color: #53AE91;
                    color: white;
                    font-weight: bold;
                    padding: 8px;
                    border: 1px solid #ddd;
                }
                .custom_table td {
                    padding: 8px;
                    border: 1px solid #ddd;
                }
                .header, .footer {
                    margin-top: 20px;
                    font-style: italic;
                }
            </style>
            """

            # Header content with Database and Account names
            Header_content = f'''
                <table style="width: 100%; border-collapse: collapse; border: 0;">
                    <tr>
                        <td style="border: none;">ROLE Snowflake Alerts </td>
                        <td style="border: none; text-align: right;">Database: {database_name}</td>
                    </tr>
                    <tr>
                        <td style="border: none;"></td>
                        <td style="border: none; text-align: right;">Account: {account_name}</td>
                    </tr>
                </table>
                <br/>
                '''

            # Footer content
            footer_content = '<div class="footer"> <br/> <br/> Disclaimer - This is a valid email notification from snowflake Instance  </div>'

            # Combine the CSS, Header, HTML table, and footer
            Body_Content = custom_css + Header_content + Body_Table + footer_content

            # Send the email
            session.call('system$send_email', 'NOTIFICATION_INT_ALL_ANALYTICS_TEAMS', send_to, subject, Body_Content, 'text/html')

            return 'Email sent:\n%s' % body
        else:
            return 'No records found to send email'
       
    except snowflake.snowpark.exceptions.SnowparkSQLException as e:
        return 'Error: %s\n%s' % (type(e), e)
$$;


call SP_EMAIL_SQLQUERY_RESULTS('username1@company.com', 'pandas to color html with header & footer with DB Name + account name' , 'select  * from ADMIN.VW_ALG_KAFKA_MONITORING_LAST_EVENT_UPDATES;' );





--------------------- ADMIN VIEW Creation Quries -----------------------
use schema IDENTIFIER($DB_Schema);
---------
create or replace VIEW  VW_QUERIES_RUNNING_GT_15_MINS as
SELECT current_account() AS SF_ACCOUNT,QUERY_ID,QUERY_TEXT,DATEDIFF('minute', START_TIME, END_TIME) AS duration_in_minutes, DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE ,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER, EXECUTION_STATUS ,  ERROR_MESSAGE ,START_TIME,END_TIME,BYTES_SCANNED,ROWS_PRODUCED as total_rows ,
ROWS_UPDATED, ROWS_INSERTED , ROWS_DELETED ,BYTES_SENT_OVER_THE_NETWORK FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
where  duration_in_minutes > 15  ;
---------
create or replace VIEW  VW_QUERIES_RUNNING_GT_30_MINS as
SELECT current_account() AS SF_ACCOUNT,QUERY_ID,QUERY_TEXT,DATEDIFF('minute', START_TIME, END_TIME) AS duration_in_minutes, DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE ,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER, EXECUTION_STATUS ,  ERROR_MESSAGE ,START_TIME,END_TIME,BYTES_SCANNED,ROWS_PRODUCED as total_rows ,
ROWS_UPDATED, ROWS_INSERTED , ROWS_DELETED ,BYTES_SENT_OVER_THE_NETWORK FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
where  duration_in_minutes > 30  ;

---------
create or replace VIEW  VW_QUERIES_RUNNING_GT_60_MINS as
SELECT current_account() AS SF_ACCOUNT,QUERY_ID,QUERY_TEXT,DATEDIFF('minute', START_TIME, END_TIME) AS duration_in_minutes, DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE ,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER, EXECUTION_STATUS ,  ERROR_MESSAGE ,START_TIME,END_TIME,BYTES_SCANNED,ROWS_PRODUCED as total_rows ,
ROWS_UPDATED, ROWS_INSERTED , ROWS_DELETED ,BYTES_SENT_OVER_THE_NETWORK FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
where  duration_in_minutes > 60   ;
---------

create or replace VIEW  VW_QUERIES_RUNNING_GT_90_MINS as
SELECT current_account() AS SF_ACCOUNT,QUERY_ID,QUERY_TEXT,DATEDIFF('minute', START_TIME, END_TIME) AS duration_in_minutes, DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE ,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER, EXECUTION_STATUS ,  ERROR_MESSAGE ,START_TIME,END_TIME,BYTES_SCANNED,ROWS_PRODUCED as total_rows ,
ROWS_UPDATED, ROWS_INSERTED , ROWS_DELETED ,BYTES_SENT_OVER_THE_NETWORK FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
where  duration_in_minutes > 90   ;
---------

create or replace VIEW  VW_QUERIES_RUNNING_GT_120_MINS as
SELECT current_account() AS SF_ACCOUNT,QUERY_ID,QUERY_TEXT,DATEDIFF('minute', START_TIME, END_TIME) AS duration_in_minutes, DATABASE_NAME,SCHEMA_NAME,QUERY_TYPE ,USER_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,WAREHOUSE_TYPE,CLUSTER_NUMBER, EXECUTION_STATUS ,  ERROR_MESSAGE ,START_TIME,END_TIME,BYTES_SCANNED,ROWS_PRODUCED as total_rows ,
ROWS_UPDATED, ROWS_INSERTED , ROWS_DELETED ,BYTES_SENT_OVER_THE_NETWORK FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
where  duration_in_minutes > 120  ;
-----------
create or replace view VW_SA_SNOWFLAKE_UI_LOGINS as 

SELECT
    event_timestamp, user_name, client_IP, reported_client_type,
    first_authentication_factor, second_authentication_factor
FROM snowflake.account_usage.login_history
WHERE second_authentication_factor IS NULL
    AND (
       reported_client_type = 'SNOWFLAKE_UI'
    OR reported_client_type = 'OTHER'
    )
    -- Searching for AccountAdmin users
    AND user_name IN (
      SELECT grantee_name
      FROM SNOWFLAKE.ACCOUNT_USAGE.grants_to_users
      WHERE user_name like 'ROLE_SF_SA%' and REPORTED_CLIENT_TYPE = 'SNOWFLAKE_UI'
        AND deleted_on IS NULL)
;

---------


use schema IDENTIFIER($DB_Schema);

create or replace view VW_USERS_CREATED_BY_ADMIN as 
SELECT
  current_account() AS SF_ACCOUNT,query_id,
  query_text,
  user_name,
  role_name,
  start_time
FROM snowflake.account_usage.query_history
WHERE query_type = 'CREATE_USER' order by START_TIME desc
; 
---------------
CREATE OR REPLACE VIEW VW_snowflake_authentication_failure_alert_query AS
SELECT
      OBJECT_CONSTRUCT('cloud', 'Snowflake', 'account', current_account()) AS environment
    , ARRAY_CONSTRUCT('snowflake') AS sources
    , 'Snowflake' AS object
    , 'Snowflake Authentication Failure' AS title
    , event_timestamp AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
    , 'User ' || USER_NAME || ' failed to authentication to Snowflake, from IP: ' || CLIENT_IP AS description
    , 'SnowAlert' AS detector
    , error_message AS event_data
    , user_name AS actor
    , 'failed to authenticate to Snowflake' AS action
    , 'Low' AS severity
    , 'c24675c89deb4e5ba6ecc57104447f90' AS query_id
    , 'snowflake_authentication_failure_alert_query' AS query_name
FROM snowflake.account_usage.login_history
WHERE 1=1
  AND IS_SUCCESS='NO' ORDER BY EVENT_TIME DESC
;
------
CREATE OR REPLACE VIEW VW_snowflake_authorization_error_alert_query COPY GRANTS AS
SELECT
      OBJECT_CONSTRUCT('cloud', 'Snowflake', 'account', current_account()) AS environment
    , ARRAY_CONSTRUCT('snowflake') AS sources
    , 'Snowflake Query' AS object
    , 'Snowflake Access Control Error' AS title
    , START_TIME AS event_time
    , current_timestamp() AS alert_time
    , 'User ' || USER_NAME || ' received ' || ERROR_MESSAGE AS description
    , 'SnowAlert' AS detector
    , ERROR_MESSAGE AS event_data
    , USER_NAME AS actor
    , 'Received an authorization error' AS action
    , 'Low' AS severity
    , 'b0724d64b40d4506b7bc4e0caedd1442' AS query_id
    , 'snowflake_authorization_error_alert_query' AS query_name
from snowflake.account_usage.query_history
WHERE 1=1
  AND error_code in (1063, 3001, 3003, 3005, 3007, 3011, 3041)
;
 ----------------------------------------------------------------Snowflake ALERTS -----------------------------------------------------
use schema IDENTIFIER($DB_Schema);

CREATE or replace ALERT ALERT_DB_LONG_RUNNING_QUERIES_GT15_MINS
 WAREHOUSE = WH_DATAARCHITECT
 SCHEDULE = '60 minute' // every 12 hrs
 IF (EXISTS (select * from QUERIES_RUNNING_GT_15_MINS where datediff(hour, start_time, CURRENT_TIMESTAMP ())<=24))  
 THEN call SP_EMAIL_SQLQUERY_RESULTS('emailuser1@company.com,emailuser1@company.com', 'Email Alert: ALERT_DB_LONG_RUNNING_QURIES > 15 Mins' , 'select current_account() AS SF_ACCOUNT,QUERY_ID,duration_in_minutes from VW_QUERIES_RUNNING_GT_15_MINS where datediff(hour, start_time, CURRENT_TIMESTAMP ())<=24 ;' );
 
 alter alert ALERT_DB_LONG_RUNNING_QUERIES_GT15_MINS resume;

---------
--https://docs.snowflake.com/en/user-guide/alerts.html#label-alerts-suspend-resume
--https://docs.snowflake.com/en/sql-reference/sql/create-alert.html
use schema IDENTIFIER($DB_Schema);

CREATE or replace ALERT ALERT_WAREHOUSE_CREDIT_USAGE
 WAREHOUSE = WH_DATAARCHITECT
 SCHEDULE = '720 minute' // every 12 hrs
 IF (EXISTS (SELECT
  Warehouse_name,
  SUM(CREDITS_USED) AS credits
  FROM "SNOWFLAKE"."ORGANIZATION_USAGE"."WAREHOUSE_METERING_HISTORY"
  // aggregate warehouse Credit_used for the past 24 hours
 WHERE datediff(hour, start_time, CURRENT_TIMESTAMP ())<=24
 GROUP BY 1
 HAVING credits > 100
 ORDER BY 2 DESC))
 THEN call SP_EMAIL_SQLQUERY_RESULTS('emailuser1@company.com', 'PROD: Excessive Snowflake warehouse usage', 'SELECT
  Warehouse_name,
  SUM(CREDITS_USED) AS credits
  FROM "SNOWFLAKE"."ORGANIZATION_USAGE"."WAREHOUSE_METERING_HISTORY"
 WHERE datediff(hour, start_time, CURRENT_TIMESTAMP ())<=24
 GROUP BY 1
 HAVING credits > 100
 ORDER BY 2 DESC' );
 
 alter alert ALERT_WAREHOUSE_CREDIT_USAGE resume;

 
 show alerts;

 
 show integrations;


-----v1 kafaka events-----Start----

create or replace view VW_ALG_KAFKA_MONITORING_LAST_EVENT_UPDATES as
SELECT CURRENT_DATABASE() as DB_Name,
    record_metadata:topic::string as from_TOPIC,
    max(CONVERT_TIMEZONE('UTC', 'America/Chicago', DATEADD('MS', record_metadata:CreateTime, '1970-01-01'))) AS TOPIC_LAST_Event_Time_CST,
    DATEDIFF(HOUR, CONVERT_TIMEZONE('UTC', 'America/Chicago', DATEADD('MS', record_metadata:CreateTime, '1970-01-01')), CURRENT_TIMESTAMP()) AS Hour_Difference
FROM
    ALG_RZ.RESERVATIONS_ALG 
WHERE 
    Hour_Difference <= 12 
group by all
ORDER BY 
    TOPIC_LAST_Event_Time_CST DESC;
    
-----v2---

--CREATE OR REPLACE VIEW VW_ALG_KAFKA_MONITORING_LAST_EVENT_UPDATES AS
WITH DataCheck AS (
        SELECT 
            MAX(CONVERT_TIMEZONE('UTC', 'America/Chicago', DATEADD('MS', record_metadata:CreateTime, '1970-01-01'))) AS Last_Message_Time_CST
        FROM
            ALG_RZ.RESERVATIONS_ALG 
    )
    SELECT CASE WHEN DATEDIFF(HOUR, Last_Message_Time_CST, CURRENT_TIMESTAMP()) > 6 THEN 'ALERT' ELSE 'OK' END AS Status
    FROM DataCheck;



select * from ADMIN.VW_ALG_KAFKA_MONITORING_LAST_EVENT_UPDATES;


CREATE or replace ALERT ALERT_KAFKA_MISSED_EVENTS_NOTIFICATION
 WAREHOUSE = WH_DATAARCHITECT
 SCHEDULE = '120 minute' // every 120 mins 
 IF (EXISTS (select * from ADMIN.VW_ALG_KAFKA_MONITORING_LAST_EVENT_UPDATES))
 THEN call SP_EMAIL_SQLQUERY_RESULTS('emailuser1@company.com', 'Snowflake Dev: Kafka events not created for more than 8 hrs', 'select  * from ADMIN.VW_ALG_KAFKA_MONITORING_LAST_EVENT_UPDATES;' );
 
 alter alert ALERT_KAFKA_MISSED_EVENTS_NOTIFICATION SUSPEND;
 show alerts;


----- kafaka events-----End----






