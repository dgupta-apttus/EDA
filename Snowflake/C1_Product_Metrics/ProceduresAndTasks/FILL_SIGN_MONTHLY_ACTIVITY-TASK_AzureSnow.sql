create or replace procedure APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY(MODE VARCHAR)
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_SIGN_MONTHLY_ACTIVITY"  
    var MODE_INNER = MODE
    var set_run = `
INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
SELECT date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_SIGN_MONTHLY_ACTIVITY' as PROC_OR_STEP
`
    var update_run_complete = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Complete'
WHERE PROC_OR_STEP = 'FILL_SIGN_MONTHLY_ACTIVITY'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_SIGN_MONTHLY_ACTIVITY')
`
    var sql_command = `
MERGE INTO APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY TARGET_T 
using ( 
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_SIGN_MONTHLY_ACTIVITY' 
)
--
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_SIGN_MONTHLY_ACTIVITY'
)
--
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL)) AS COMPLETED_MONTH
             , YEAR(COMPLETED_MONTH) AS REPORT_YEAR
             , MONTH(COMPLETED_MONTH) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE
)
, EDIT_SENT_EVENTS AS (
        SELECT SYSTEM_TYPE AS ORG_SOURCE
             , CASE
                 WHEN SYSTEM_TYPE <> 'COLLABORATE'
                   THEN SYSTEM_ID 
                 WHEN SYSTEM_TYPE = 'COLLABORATE'
                  AND SYSTEM_ID  LIKE 'collaborate-production-%'
                   THEN SUBSTRING(SYSTEM_ID ,24)
                else SYSTEM_ID         
               END AS SOURCE_ORG_ID
             , YEAR(REQUEST_TIMESTAMP) AS ACTIVITY_YEAR
             , MONTH(REQUEST_TIMESTAMP) AS ACTIVITY_MONTH
             , SENDER_ID
             , CASE 
                 WHEN SYSTEM_ENVIRONMENT <> 'SANDBOX'
                   THEN 0::BOOLEAN
                 ELSE 1::BOOLEAN
               END AS IS_SANDBOX_EDITION                      
        FROM APTTUS_DW.SF_PRODUCTION.SIGN_SIGNINGREQUEST_EVENT 
        WHERE EVENT_TYPE = 'SENT'
          and Year(REQUEST_TIMESTAMP) = (select REPORT_YEAR from SET_DATE_RANGE)
          and Month(REQUEST_TIMESTAMP) = (select REPORT_MONTH from SET_DATE_RANGE)
)        
, GROUP_EVENTS AS (
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , COUNT(*) AS ACTIVITY_COUNT
             , COUNT(DISTINCT SENDER_ID) AS UNIQUE_USERS
             , BOOLAND_AGG(IS_SANDBOX_EDITION) AS IS_SANDBOX_EDITION                      
        FROM EDIT_SENT_EVENTS
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH               
)
        SELECT A.ORG_SOURCE
             , A.SOURCE_ORG_ID
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE 
             , 'Conga Sign' as PRODUCT_LINE             
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION    
        FROM                   GROUP_EVENTS A      
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1 
) SOURCE_T
    ON  TARGET_T.ORG_SOURCE = SOURCE_T.ORG_SOURCE
    AND TARGET_T.SOURCE_ORG_ID = SOURCE_T.SOURCE_ORG_ID
    AND TARGET_T.ACTIVITY_MONTH_DATE = SOURCE_T.ACTIVITY_MONTH_DATE
    AND TARGET_T.PRODUCT_LINE = SOURCE_T.PRODUCT_LINE
WHEN NOT MATCHED THEN 
INSERT 
(              ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , ACTIVITY_MONTH_DATE 
             , PRODUCT_LINE
             , ACTIVITY_COUNT
             , UNIQUE_USERS
             , IS_SANDBOX_EDITION
             , ACTIVITY_ACCOUNT_ID
             , ACTIVITY_ACCOUNT_NAME
) VALUES
(              SOURCE_T.ORG_SOURCE
             , SOURCE_T.SOURCE_ORG_ID
             , SOURCE_T.ACTIVITY_YEAR
             , SOURCE_T.ACTIVITY_MONTH
             , SOURCE_T.ACTIVITY_MONTH_DATE 
             , SOURCE_T.PRODUCT_LINE
             , SOURCE_T.ACTIVITY_COUNT
             , SOURCE_T.UNIQUE_USERS
             , SOURCE_T.IS_SANDBOX_EDITION 
             , null
             , null 
)
`
// execute SQLs
    var stepname = "Set Run Parameters in PRODUCT_METRICS_RUN_CONTROL"
    var error_code = 0
    if (MODE_INNER == 'Full'){
	    try {
	        snowflake.execute (
	            {sqlText: set_run}
	            );
	         return_value = "Succeeded.";   // Return a success/error indicator.
	         snowflake.execute({
	                    sqlText: `insert into APTTUS_DW.PRODUCT.ACTIVITY_LOG (procedure_name, step_name) VALUES (?,?)`
	                    ,binds: [procname, stepname]
	                    });         
	        }
	    catch (err)  {
	                var errorstr = err.message.replace(/\n/g, " ")
	                return_value = "Failed: " + errorstr + " Code: " + err.code + " State: " + err.state;
	                snowflake.execute({
	                    sqlText: `insert into APTTUS_DW.PRODUCT.ACTIVITY_LOG VALUES (?,?,?,?,?,?,current_user(),CONVERT_TIMEZONE('UTC',current_timestamp()))`
	                    ,binds: [procname, stepname, err.code, err.state, errorstr, err.stackTraceTxt]
	                    });
	                return return_value;
	            };
    }; // end of if
    var stepname = "Merge PIPELINED_MONTHLY_ACTIVITY from SIGN_SIGNINGREQUEST_EVENT" 
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
         return_value = "Succeeded.";   // Return a success/error indicator.
        snowflake.execute (
                    {sqlText: update_run_complete}
                    );
         snowflake.execute({
                    sqlText: `insert into APTTUS_DW.PRODUCT.ACTIVITY_LOG (procedure_name, step_name, error_code, error_message) VALUES (?,?,?,?)`
                    ,binds: [procname, stepname, error_code, return_value]
                    });              
        }
    catch (err)  {
                var errorstr = err.message.replace(/\n/g, " ")
                return_value = "Failed: " + errorstr + " Code: " + err.code + " State: " + err.state;
                snowflake.execute({
                    sqlText: `insert into APTTUS_DW.PRODUCT.ACTIVITY_LOG VALUES (?,?,?,?,?,?,current_user(),CONVERT_TIMEZONE('UTC',current_timestamp()))`
                    ,binds: [procname, stepname, err.code, err.state, errorstr, err.stackTraceTxt]
                    });
            };

    return return_value;
    $$
    ;       
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY(VARCHAR);
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY('Full'); -- the other option is manual     
-- to run in manual mode
-- first if they exist already, delete all records from APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY that have a month 
-- and a product line or package that you want to replace or rerun
-- second run below statement to insert a control record replacing the RUN_FOR_DATE with the month you want to run
--INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (PROC_OR_STEP, RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS)
--SELECT 'FILL_SIGN_MONTHLY_ACTIVITY' as PROC_OR_STEP
--     ,  date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
--     , CURRENT_USER() AS OPERATOR
--     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
--     , 'Incomplete' AS STATUS
--;
-- then run with Manual mode so it uses the control you just created rather than generating a new one
-- if there are duplicates after your run then something has gone terribly wrong -- figure out which months have duplicates delete and rerun those
-- a delete statement might look something like 
---- --DELETE from APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY_TESTING
--   --where  PRODUCT_LINE = 'Conga Sign'
--   --  and ACTIVITY_MONTH_DATE = '2020-09-01'  
CALL APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY('Manual');

       
CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 18 16 01 * * UTC' -- after sign 
AS CALL APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY('Full')
; 
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY;
alter task APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY resume;

show tasks IN SCHEMA PRODUCT;
SHOW procedures IN SCHEMA PRODUCT;
--DROP procedure APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY();
