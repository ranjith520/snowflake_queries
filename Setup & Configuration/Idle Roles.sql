/* Description:
Roles that have not been used in the last 30 days

How to Interpret Results:
Are these roles necessary? Should these roles be cleaned up?

Primary Schema:
Account_Usage

SQL */
SELECT 
	R.*
FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES R
LEFT JOIN (
    SELECT DISTINCT 
        ROLE_NAME 
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
    WHERE START_TIME > DATEADD(month,-1,CURRENT_TIMESTAMP())
        ) Q 
                ON Q.ROLE_NAME = R.NAME
WHERE Q.ROLE_NAME IS NULL
and DELETED_ON IS NULL;