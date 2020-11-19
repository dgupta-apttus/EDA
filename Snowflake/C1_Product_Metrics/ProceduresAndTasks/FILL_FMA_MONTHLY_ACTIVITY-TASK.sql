create or replace procedure APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY()
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_FMA_MONTHLY_ACTIVITY"
    var stepname = "Insert FMA_MONTHLY_ACTIVITY from FMA_Rolling_Activity"    
    var sql_command = `
INSERT INTO APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY 
(ORG_SOURCE, SOURCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH, ACTIVITY_MONTH_DATE, PRODUCT_LINE, ACTIVITY_COUNT, UNIQUE_USERS, LICENSE_ID)
with step1 as ( 
        SELECT SOURCE_ORG_ID
             , Year(ACTIVITY_DATE) AS ACTIVITY_YEAR
             , Month(ACTIVITY_DATE) AS ACTIVITY_MONTH
             , PRODUCT_LINE
             , LICENSE_ID
             , MAX(ROLLING_ACTIVITY_COUNT) AS ACTIVITY_COUNT
             , MAX(ROLLING_ACTIVE_USERS) AS UNIQUE_USERS
             , SUM(CONTRACTS4SF_DAILY_ACTIVITY) AS ACTIVITY2
        FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
        where Year(ACTIVITY_DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(ACTIVITY_DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))
        group by SOURCE_ORG_ID
             , Year(ACTIVITY_DATE)
             , Month(ACTIVITY_DATE)
             , PRODUCT_LINE 
             , LICENSE_ID  
)
        SELECT 'SALESFORCE' AS ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE             
             , PRODUCT_LINE
             , CASE 
                 WHEN PRODUCT_LINE = 'Conga Contracts for Salesforce'
                   THEN ACTIVITY2
                ELSE ACTIVITY_COUNT
               END AS ACTIVITY_COUNT
             , UNIQUE_USERS                   
             , LICENSE_ID
             -- get sandbox or non production from license later
             
        FROM                   step1 A
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1 
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
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY();
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY();       
       
CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 01 07 01 * * UTC' -- 7:01 am UTC time on first day of month
AS CALL APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY()
;
/* potential body of task but moved to a proc
INSERT INTO FMA_MONTHLY_ACTIVITY 
(ORG_SOURCE, SOURCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH, ACTIVITY_MONTH_DATE, PRODUCT_LINE, ACTIVITY_COUNT, UNIQUE_USERS, LICENSE_ID)
with step1 as ( 
        SELECT SOURCE_ORG_ID
             , Year(ACTIVITY_DATE) AS ACTIVITY_YEAR
             , Month(ACTIVITY_DATE) AS ACTIVITY_MONTH
             , PRODUCT_LINE
             , LICENSE_ID
             , MAX(ROLLING_ACTIVITY_COUNT) AS ACTIVITY_COUNT
             , MAX(ROLLING_ACTIVE_USERS) AS UNIQUE_USERS
             , SUM(CONTRACTS4SF_DAILY_ACTIVITY) AS ACTIVITY2
        FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
        where Year(ACTIVITY_DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(ACTIVITY_DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))
        group by SOURCE_ORG_ID
             , Year(ACTIVITY_DATE)
             , Month(ACTIVITY_DATE)
             , PRODUCT_LINE 
             , LICENSE_ID  
)
        SELECT 'SALESFORCE' AS ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE             
             , PRODUCT_LINE
             , CASE 
                 WHEN PRODUCT_LINE = 'Conga Contracts for Salesforce'
                   THEN ACTIVITY2
                ELSE ACTIVITY_COUNT
               END AS ACTIVITY_COUNT
             , UNIQUE_USERS                   
             , LICENSE_ID
             -- get sandbox or non production from license later
             
        FROM                   step1 A
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1     
;
*/   
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY;
alter task APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY resume;

show tasks IN SCHEMA PRODUCT;

/* create table from history
--drop table APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY;

create table APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY
as -- 62978 loaded as history from Redshift
SELECT ORG_SOURCE
     , SOURCE_ORG_ID
     , ACTIVITY_YEAR
     , ACTIVITY_MONTH
     , ACTIVITY_MONTH_DATE
     , PRODUCT_LINE
     , ACTIVITY_COUNT
     , UNIQUE_USERS
     , NULL::varchar(16777216) AS LICENSE_ID 
     --, IS_SANDBOX_EDITION
     --, ENVIRONMENT_ID
     --, ACTIVITY_MONTH_DATE_ID 
     FROM APTTUS_DW.PRODUCT.ACTIVITY_MONTHLY_SUMMARY_RSH
     WHERE PRODUCT_LINE in ('Conga Grid','Conga Contracts for Salesforce','Conga Orchestrate')
;
*/
