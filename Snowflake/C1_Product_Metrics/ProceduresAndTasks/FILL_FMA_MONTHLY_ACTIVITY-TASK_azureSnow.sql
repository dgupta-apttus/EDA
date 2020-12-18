--DROP procedure APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY(); -- old had no param

create or replace procedure APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY(MODE VARCHAR)
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_FMA_MONTHLY_ACTIVITY"
    var MODE_INNER = MODE    
    var set_run = `
INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
SELECT date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_FMA_MONTHLY_ACTIVITY' as PROC_OR_STEP
`
    var update_run_complete = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Complete'
WHERE PROC_OR_STEP = 'FILL_FMA_MONTHLY_ACTIVITY'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_FMA_MONTHLY_ACTIVITY')
`
    var sql_command = `
INSERT INTO APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY 
(ORG_SOURCE, SOURCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH, ACTIVITY_MONTH_DATE, PRODUCT_LINE, ACTIVITY_COUNT, UNIQUE_USERS, LICENSE_ID)
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_FMA_MONTHLY_ACTIVITY' 
)
--
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_FMA_MONTHLY_ACTIVITY'
)
-- 
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL)) AS COMPLETED_MONTH
             , YEAR(COMPLETED_MONTH) AS REPORT_YEAR
             , MONTH(COMPLETED_MONTH) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE
)             
, step1 as ( 
        SELECT SOURCE_ORG_ID
             , Year(ACTIVITY_DATE) AS ACTIVITY_YEAR
             , Month(ACTIVITY_DATE) AS ACTIVITY_MONTH
             , PRODUCT_LINE
             , LICENSE_ID
             , MAX(ROLLING_ACTIVITY_COUNT) AS ACTIVITY_COUNT
             , MAX(ROLLING_ACTIVE_USERS) AS UNIQUE_USERS
             , SUM(CONTRACTS4SF_DAILY_ACTIVITY) AS ACTIVITY2
        FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
        where Year(ACTIVITY_DATE) = (select REPORT_YEAR from SET_DATE_RANGE)
          and Month(ACTIVITY_DATE) = (select REPORT_MONTH from SET_DATE_RANGE)        
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
// execute SQLs
    var stepname = "Set Run Parameters for FILL_FMA_MONTHLY_ACTIVITY in PRODUCT_METRICS_RUN_CONTROL"
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

    var stepname = "Insert FMA_MONTHLY_ACTIVITY from FMA_Rolling_Activity"
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
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY(VARCHAR);
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY('Full');  -- the other option is manual     
-- to run in manual mode
-- first if they exist already, delete all records from APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY that have a month 
--  that you want to replace or rerun
-- second run below statement to insert a control record replacing the RUN_FOR_DATE with the month you want to run
--INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (PROC_OR_STEP, RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS)
--SELECT 'FILL_FMA_MONTHLY_ACTIVITY' as PROC_OR_STEP
--     ,  date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
--     , CURRENT_USER() AS OPERATOR
--     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
--     , 'Incomplete' AS STATUS
--;
-- then run with Manual mode so it uses the control you just created rather than generating a new one
-- if there are duplicates after your run then something has gone terribly wrong -- figure out which months have duplicates delete and rerun those
-- a delete statement might look something like 
---- --DELETE from APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY
--   --where ACTIVITY_MONTH_DATE = '2020-09-01'  
--CALL APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY('Manual');     
       
CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 17 02 01 * * America/Los_Angeles' -- 7:01 am UTC time on first day of month
AS CALL APTTUS_DW.PRODUCT.FILL_FMA_MONTHLY_ACTIVITY('Full')
;
 
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
