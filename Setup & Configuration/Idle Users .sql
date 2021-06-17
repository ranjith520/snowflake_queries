/* Description:
Users in the Snowflake platform that have not logged in in the last 30 days

How to Interpret Results:
Should these users be removed or more formally onboarded?

Primary Schema:
Account_Usage

SQL */
SELECT 
	*
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS 
WHERE LAST_SUCCESS_LOGIN < DATEADD(month, -1, CURRENT_TIMESTAMP()) 
AND DELETED_ON IS NULL;