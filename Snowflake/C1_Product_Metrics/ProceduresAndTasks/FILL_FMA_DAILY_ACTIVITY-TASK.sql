create or replace procedure APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY()
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_FMA_DAILY_ACTIVITY"
    var stepname = "Insert FMA_DAILY_ACTIVITY from FMA_Rolling_Activity"    
    var sql_command = `
MERGE INTO APTTUS_DW.PRODUCT.FMA_DAILY_ACTIVITY TARGET_T 
using (
        SELECT SOURCE_ORG_ID
             , ACTIVITY_DATE 
             , PRODUCT_LINE
             , LICENSE_ID
             , CASE 
                 WHEN PRODUCT_LINE = 'Conga Contracts for Salesforce'
                   THEN CONTRACTS4SF_DAILY_ACTIVITY
                ELSE ROLLING_ACTIVITY_COUNT
               END AS ACTIVITY_COUNT
             , ROLLING_ACTIVE_USERS as UNIQUE_USERS 
        FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
        WHERE ACTIVITY_DATE > CURRENT_DATE() - 10   
) SOURCE_T
    ON TARGET_T.SOURCE_ORG_ID = SOURCE_T.SOURCE_ORG_ID
    AND TARGET_T.ACTIVITY_DATE = SOURCE_T.ACTIVITY_DATE
    AND TARGET_T.PRODUCT_LINE = SOURCE_T.PRODUCT_LINE
    AND TARGET_T.LICENSE_ID = SOURCE_T.LICENSE_ID
WHEN NOT MATCHED THEN 
INSERT 
(     SOURCE_ORG_ID
    , ACTIVITY_DATE 
    , PRODUCT_LINE
    , LICENSE_ID 
    , ACTIVITY_COUNT 
    , UNIQUE_USERS
) VALUES ( 
      SOURCE_T.SOURCE_ORG_ID
    , SOURCE_T.ACTIVITY_DATE 
    , SOURCE_T.PRODUCT_LINE
    , SOURCE_T.LICENSE_ID 
    , SOURCE_T.ACTIVITY_COUNT 
    , SOURCE_T.UNIQUE_USERS
)
WHEN MATCHED THEN 
UPDATE SET TARGET_T.ACTIVITY_COUNT = SOURCE_T.ACTIVITY_COUNT, TARGET_T.UNIQUE_USERS = SOURCE_T.UNIQUE_USERS
`
    try {
        snowflake.execute (
            {sqlText: sql_command}
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
            };

    return return_value;
    $$
    ;       
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY();
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY();       
       
CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 15 02 * * * America/Los_Angeles'  -- must be after the FMA Snapshot
AS CALL APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY()
;
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY;
alter task APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_FMA_DAILY_ACTIVITY resume;

show tasks IN SCHEMA PRODUCT;

/* create table from history
--drop table APTTUS_DW.PRODUCT.FMA_DAILY_ACTIVITY;

CREATE TABLE APTTUS_DW.PRODUCT.FMA_DAILY_ACTIVITY
(     SOURCE_ORG_ID
    , ACTIVITY_DATE 
    , PRODUCT_LINE
    , LICENSE_ID 
    , ACTIVITY_COUNT 
    , UNIQUE_USERS
)            
AS
        SELECT SOURCE_ORG_ID
             , ACTIVITY_DATE 
             , PRODUCT_LINE
             , LICENSE_ID
             , CASE 
                 WHEN PRODUCT_LINE = 'Conga Contracts for Salesforce'
                   THEN CONTRACTS4SF_DAILY_ACTIVITY
                ELSE ROLLING_ACTIVITY_COUNT
               END AS ACTIVITY_COUNT
             , ROLLING_ACTIVE_USERS as UNIQUE_USERS 
        FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
; 
*/
