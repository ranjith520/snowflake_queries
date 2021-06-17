/* Description:
Users that have never logged in to Snowflake

How to Interpret Results:
Should these users be removed or more formally onboarded?

Primary Schema:
Account_Usage

SQL */
SELECT 
	*
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS 
WHERE LAST_SUCCESS_LOGIN IS NULL;