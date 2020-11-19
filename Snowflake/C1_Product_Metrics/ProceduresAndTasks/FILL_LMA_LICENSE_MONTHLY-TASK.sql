create or replace procedure APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY(MODE VARCHAR)
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_LMA_LICENSE_MONTHLY"
    var stepname = "Insert LMA_LICENSE_MONTHLY from LMA_LICENSE_C1_HISTORY"    
    var MODE_INNER = MODE
    var set_run = `
INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
SELECT date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_LMA_LICENSE_MONTHLY' as PROC_OR_STEP
` 
    var check_dups = `
select count(*) as DUP_COUNT from (
SELECT COUNT(*) , CUSTOMER_ORG_18, PRODUCT_LINE, REPORTING_MONTH 
FROM APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY
GROUP BY CUSTOMER_ORG_18, PRODUCT_LINE, REPORTING_MONTH 
HAVING COUNT(*) > 1
)
`
    var update_run_complete = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Complete'
WHERE PROC_OR_STEP = 'FILL_LMA_LICENSE_MONTHLY'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_LMA_LICENSE_MONTHLY')
`
    var update_run_dups = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Duplicates Created'
WHERE PROC_OR_STEP = 'FILL_LMA_LICENSE_MONTHLY'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_LMA_LICENSE_MONTHLY')
`
    var sql_command = `
INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_LMA_LICENSE_MONTHLY' 
)
--
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_LMA_LICENSE_MONTHLY' 
)
--
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL)) AS COMPLETED_MONTH
             , YEAR(COMPLETED_MONTH) AS REPORT_YEAR
             , MONTH(COMPLETED_MONTH) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE
)
, LISTS AS (
        select    listagg(DISTINCT PACKAGE_NAME, ', ') within group (ORDER BY PACKAGE_SORT) AS PACKAGE_LIST 
                , listagg(DISTINCT PACKAGE_ID, ', ') within group (ORDER BY PACKAGE_ID) AS PACKAGE_ID_LIST
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS LICENSE_ID_LIST
                , CUSTOMER_ORG_18
                , PRODUCT_LINE
                , COUNT(*) AS LICENSE_COUNT
                , COUNT(DISTINCT PACKAGE_ID) AS PACKAGE_COUNT
                , MAX(MONTHS_INSTALLED) AS LONGEST_INSTALL 
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE UPPER(STATUS) = 'ACTIVE'
        group by CUSTOMER_ORG_18, PRODUCT_LINE
) 
select  
          A.CUSTOMER_ORG_18
        , A.PRODUCT_LINE
        , (SELECT COMPLETED_MONTH FROM SET_DATE_RANGE) AS REPORTING_MONTH -- set to previous month
        , A.LICENSE_ID AS PRIMARY_LICENSE_ID
        , COALESCE(B.LICENSE_ID_LIST, 'None Active') AS LICENSE_ID_LIST
        , COALESCE(B.LICENSE_COUNT, 0) AS LICENSE_COUNT
        , A.CUSTOMER_ORG_15  
        , A.PACKAGE_NAME
        , COALESCE(B.PACKAGE_LIST, 'None Active') AS PACKAGE_LIST
        , A.PACKAGE_ID
        , COALESCE(B.PACKAGE_ID_LIST, 'None Active') AS PACKAGE_ID_LIST
        , COALESCE(B.PACKAGE_COUNT, 0) AS PACKAGE_COUNT
        , PACKAGE_VERSION_ID
        , ORG_PACKAGE
        , STATUS
        , ORG_STATUS 
        , ACCOUNT_ID
        , ACCOUNT_NAME  
        , IS_SANDBOX
        , PREDICTED_PACKAGE_NAMESPACE 
        , LICENSE_SEAT_TYPE    
        , SEATS 
        , USED_LICENSES        
        , INSTALL_DATE
        , UNINSTALL_DATE
        , MONTHS_INSTALLED
        , INSTALL_DATE_STRING
        , COALESCE(B.LONGEST_INSTALL, MONTHS_INSTALLED) AS LONGEST_INSTALL          
        , EXPIRATION_DATE
        , EXPIRATION_DATE_STRING                           
        , LAST_ACTIVITY_DATE
        , SUSPEND_ACCOUNT_BOOL
        , ACCOUNT_SUSPENDED_REASON
        , LICENSE_NAME
        , C1_PRODUCTION_BOOL
        , 'C1_Snapshots' AS DATA_SOURCE
FROM                          APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT A
LEFT OUTER JOIN               LISTS B
              ON  A.CUSTOMER_ORG_18 = B.CUSTOMER_ORG_18
              AND A.PRODUCT_LINE = B.PRODUCT_LINE
WHERE A.SELECT1_FOR_PRODUCT_LINE = 1
 AND (   LAST_ACTIVITY_DATE >= (CURRENT_DATE()-75)
      OR (     LAST_ACTIVITY_DATE < (CURRENT_DATE()-75)
          AND  UPPER(STATUS) = 'ACTIVE'
          AND  EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED', 'EXPIRED') 
         ) 
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
    var stepname = "Insert LMA_LIC_PRODUCTLINE_MONTHLY from MONTHLY_ACTIVITY"  
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return_value = "Succeeded.";   // Return a success/error indicator.

	    var stmt1 = snowflake.createStatement({sqlText: check_dups});
	    var RS1 = stmt1.execute();
	    RS1.next();
	    var DUP_COUNT = RS1.getColumnValue(1);
        if (DUP_COUNT != 0){
			return_value = "Rows Inserted to LMA_LIC_PRODUCTLINE_MONTHLY but duplicate values found"
            error_code = 1
            snowflake.execute (
                {sqlText: update_run_dups}
                );
	    } else {
        snowflake.execute (
            {sqlText: update_run_complete}
            );
        }; 
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
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY(varchar);
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY('Full'); -- the other option is manual     
-- to run in manual mode (this one can only be run or rerun for the recently completed month since it refences the Current views for license 
-- as a future enhancement this could be built to run date driven off of the LMA_LICENSE HISTORY 
-- to rerun for the current month
-- first if they exist already,  delete all records from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY for the recently completed month
-- <future enhandment only > second run below statement to insert a control record replacing the RUN_FOR_DATE with the month you want to run
--INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (PROC_OR_STEP, RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS)
--SELECT 'FILL_LMA_LICENSE_MONTHLY' as PROC_OR_STEP
--     ,  date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
--     , CURRENT_USER() AS OPERATOR
--     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
--     , 'Incomplete' AS STATUS
--;
-- then run with Manual mode so it uses the control you just created rather than generating a new one
-- if there are duplicates after your run then something has gone terribly wrong -- figure out which months have duplicates delete and rerun those
-- a delete statement might look something like 
---- --DELETE FROM LMA_LIC_PRODUCTLINE_MONTHLY WHERE REPORT_DATE = '2035-08-01';  
CALL APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY('Manual');

CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 12 16 01 * * UTC' -- after LMA snapshots
AS CALL APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY('Full')
; 
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY;
alter task APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_LMA_LICENSE_MONTHLY resume;

show tasks IN SCHEMA PRODUCT;
show procedures IN SCHEMA PRODUCT;



