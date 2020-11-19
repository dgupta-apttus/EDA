create or replace procedure APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY()
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_SIGN_MONTHLY_ACTIVITY"
    var stepname = "Merge PIPELINED_MONTHLY_ACTIVITY from SIGN_SIGNINGREQUEST_EVENT"    
    var sql_command = `
MERGE INTO APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY TARGET_T 
using (
WITH EDIT_SENT_EVENTS AS (
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
          and Year(REQUEST_TIMESTAMP) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(REQUEST_TIMESTAMP) = MONTH(dateadd(month, -1, CURRENT_DATE()))
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
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
         return_value = "Succeeded.";   // Return a success/error indicator.
         snowflake.execute({
                    sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG (procedure_name, step_name) VALUES (?,?)`
                    ,binds: [procname, stepname]
                    });         
        }
    catch (err)  {
                var errorstr = err.message.replace(/\n/g, " ")
                return_value = "Failed: " + errorstr + " Code: " + err.code + " State: " + err.state;
                snowflake.execute({
                    sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG VALUES (?,?,?,?,?,?,current_user(),CONVERT_TIMEZONE('UTC',current_timestamp()))`
                    ,binds: [procname, stepname, err.code, err.state, errorstr, err.stackTraceTxt]
                    });
            };

    return return_value;
    $$
    ;       
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY();
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY();       
       
CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 18 16 01 * * UTC' -- after sign 
AS CALL APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY()
; 
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY;
alter task APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_SIGN_MONTHLY_ACTIVITY resume;

show tasks IN SCHEMA PRODUCT;

