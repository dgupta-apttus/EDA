create or replace procedure APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY()
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_CONTRACTS_MONTHLY_ACTIVITY"
    var stepname = "Merge PIPELINED_MONTHLY_ACTIVITY from joined CONTRACTS Objects"    
    var sql_command = `
MERGE INTO APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY TARGET_T 
using (
WITH GET_CONTRACT_ACTIVITY AS (
        SELECT USER_COMPANY_UUID
             , Year(REPORT_DATE) AS ACTIVITY_YEAR
             , Month(REPORT_DATE) AS ACTIVITY_MONTH
             , COUNT(DISTINCT CONTRACT_UUID) AS ACTIVITY_COUNT
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_CONTRACT_COUNTS  
        WHERE Year(REPORT_DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(REPORT_DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))          
        GROUP BY USER_COMPANY_UUID, ACTIVITY_YEAR, ACTIVITY_MONTH        
)  
, GET_CONTRACT_USERS AS (
       SELECT USER_COMPANY_UUID
             , Year(REPORT_DATE) AS ACTIVITY_YEAR
             , Month(REPORT_DATE) AS ACTIVITY_MONTH
             , COUNT(DISTINCT APP_USER_UUID) AS UNIQUE_USERS
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_LOGINS
        WHERE Year(REPORT_DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(REPORT_DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))          
        GROUP BY USER_COMPANY_UUID, ACTIVITY_YEAR, ACTIVITY_MONTH        
)  
, GET_COMPANY AS (
        SELECT USER_COMPANY_UUID
             , ENVIRONMENT
             , CASE 
                 WHEN UPPER(ENVIRONMENT) LIKE '%SANDBOX%'
                   THEN 1::BOOLEAN
                 WHEN UPPER(ENVIRONMENT) LIKE '%DEMO%'                   
                   THEN 1::BOOLEAN
                ELSE 0::BOOLEAN
               END AS IS_SANDBOX_EDITION 
             , COMPANY_NAME  
        FROM APTTUS_DW.PRODUCT.CONTRACTS_CLIENT_CONFIGURATION_CURRENT -- THIS IS A VIEW
)
, GET_ACTY_USERS AS (
        SELECT COALESCE(A.USER_COMPANY_UUID, B.USER_COMPANY_UUID) AS USER_COMPANY_UUID
             , COALESCE(A.ACTIVITY_YEAR, B.ACTIVITY_YEAR) AS ACTIVITY_YEAR
             , COALESCE(A.ACTIVITY_MONTH, B.ACTIVITY_MONTH) AS ACTIVITY_MONTH
             , COALESCE(A.ACTIVITY_COUNT, 0) AS ACTIVITY_COUNT
             , COALESCE(B.UNIQUE_USERS, 0) AS UNIQUE_USERS
             , C.IS_SANDBOX_EDITION
             , C.COMPANY_NAME           
        FROM                GET_CONTRACT_ACTIVITY A
        FULL OUTER JOIN     GET_CONTRACT_USERS B
                        ON  A.USER_COMPANY_UUID = B.USER_COMPANY_UUID
                        AND A.ACTIVITY_YEAR = B.ACTIVITY_YEAR
                        AND A.ACTIVITY_MONTH = B.ACTIVITY_MONTH
        INNER JOIN          GET_COMPANY C   
                        ON  COALESCE(A.USER_COMPANY_UUID, B.USER_COMPANY_UUID) = C.USER_COMPANY_UUID
)
        SELECT 'CONTRACTS' AS ORG_SOURCE
             , A.USER_COMPANY_UUID AS SOURCE_ORG_ID 
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE
             , 'Conga Contracts' AS PRODUCT_LINE
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION
             , NULL as ACTIVITY_ACCOUNT_ID
             , A.COMPANY_NAME AS ACTIVITY_ACCOUNT_NAME             
        FROM                    GET_ACTY_USERS A
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
             , SOURCE_T.ACTIVITY_ACCOUNT_ID
             , SOURCE_T.ACTIVITY_ACCOUNT_NAME 
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
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY();
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY();       
       
CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 21 16 01 * * UTC' -- after CONTRACTS 
AS CALL APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY()
; 
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY;
alter task APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_CONTRACTS_MONTHLY_ACTIVITY resume;

show tasks IN SCHEMA PRODUCT;

