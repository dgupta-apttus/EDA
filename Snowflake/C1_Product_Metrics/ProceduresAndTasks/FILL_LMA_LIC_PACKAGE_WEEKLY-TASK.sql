create or replace procedure APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY(MODE VARCHAR)
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_LMA_LIC_PACKAGE_WEEKLY"
    var MODE_INNER = MODE
    var set_run = `
INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, RUN_DATE, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
SELECT date_trunc('MONTH', CURRENT_DATE()) AS RUN_FOR_MONTH
     , dateadd(day, -1, CURRENT_DATE()) AS RUN_DATE -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_LMA_LIC_PACKAGE_WEEKLY' as PROC_OR_STEP
` 
    var check_dups = `
select count(*) as DUP_COUNT from (
SELECT COUNT(*) , CUSTOMER_ORG, PACKAGE_ID, REPORTING_DATE
FROM APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_WEEKLY
GROUP BY CUSTOMER_ORG, PACKAGE_ID, REPORTING_DATE
HAVING COUNT(*) > 1
)
`
    var update_run_complete = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Complete'
WHERE PROC_OR_STEP = 'FILL_LMA_LIC_PACKAGE_WEEKLY'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_LMA_LIC_PACKAGE_WEEKLY')
`
    var update_run_dups = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Duplicates Created'
WHERE PROC_OR_STEP = 'FILL_LMA_LIC_PACKAGE_WEEKLY'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_LMA_LIC_PACKAGE_WEEKLY')
`
    var sql_command = `
INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_WEEKLY
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_LMA_LIC_PACKAGE_WEEKLY' 
)
--
, DATE_FROM_CONTROL AS (
                SELECT RUN_DATE
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_LMA_LIC_PACKAGE_WEEKLY'
)
--
, ACTIVE_LIC_TYPES as ( 
        SELECT count(*) as LICENSES_WSEATS
             , CUSTOMER_ORG
             ,  PACKAGE_ID  
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
          AND LICENSE_SEAT_TYPE = 'Seats'
        group by CUSTOMER_ORG, PACKAGE_ID  
)
, ACTIVE_LISTS AS (
        select    listagg(DISTINCT PACKAGE_NAME, ', ') within group (ORDER BY PACKAGE_NAME) AS PACKAGE_LIST 
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS LICENSE_ID_LIST
                , CUSTOMER_ORG
                , PACKAGE_ID
                , COUNT(*) AS LICENSE_COUNT
                , MAX(MONTHS_INSTALLED) AS LONGEST_INSTALL
                , CASE
                    WHEN COUNT(DISTINCT LICENSE_SEAT_TYPE) < 2
                      then listagg(DISTINCT LICENSE_SEAT_TYPE , ', ') within group (ORDER BY LICENSE_SEAT_TYPE) 
                   ELSE 'Mixed'    
                  END AS ACTIVE_SEAT_TYPE 
                , SUM(SEATS) as ACTIVE_SEATS
                , SUM(USED_LICENSES) AS ACTIVE_USED
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
        group by CUSTOMER_ORG, PACKAGE_ID
)
, NONPRODUCTION AS (
        select    CUSTOMER_ORG
                , PACKAGE_ID
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS NONPROD_LICENSE_ID_LIST
                , COUNT(*) AS NONPROD_LICENSE_COUNT
                , SUM(SEATS) as NONPROD_SEATS
                , SUM(USED_LICENSES) AS NONPROD_USED
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              ) 
        group by CUSTOMER_ORG, PACKAGE_ID
)
, SANDBOX AS (
        select    CUSTOMER_ORG
                , PACKAGE_ID
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS SANDBOX_LICENSE_ID_LIST
                , COUNT(*) AS SANDBOX_LICENSE_COUNT
                , SUM(SEATS) as SANDBOX_SEATS
                , SUM(USED_LICENSES) AS SANDBOX_USED
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE IS_SANDBOX = true
        group by CUSTOMER_ORG, PACKAGE_ID
)
        select    A.CUSTOMER_ORG  
                , A.PRODUCT 
                , A.PRODUCTFAMILY
                , (SELECT RUN_DATE FROM DATE_FROM_CONTROL) AS REPORTING_DATE    
                , A.CUSTOMER_ORG_15
                , A.CUSTOMER_ORG_18  
                , A.PACKAGE_NAME
                , COALESCE(B.PACKAGE_LIST, 'None Active') AS ACTIVE_PACKAGE_LIST
                , A.PACKAGE_ID
                , PACKAGE_VERSION_ID
                , ORG_PACKAGE
                , COALESCE(B.ACTIVE_SEAT_TYPE, 'Not Production') AS ACTIVE_SEAT_TYPE               
                , LICENSE_SEAT_TYPE                              AS PRIMARY_ROW_SEAT_TYPE
                , COALESCE(B.LICENSE_COUNT, 0)                   AS ACTIVE_LICENSE_COUNT
                , COALESCE(C.LICENSES_WSEATS, 0)                 AS ACTIVE_LICENSES_WSEATS 
                , COALESCE(D.NONPROD_LICENSE_COUNT, 0)           AS NONPROD_LICENSE_COUNT
                , COALESCE(E.SANDBOX_LICENSE_COUNT, 0)           AS SANDBOX_LICENSE_COUNT
                , COALESCE(B.ACTIVE_SEATS, 0)                    AS ACTIVE_SEATS
                , COALESCE(B.ACTIVE_USED, 0)                     AS ACTIVE_USED
                , COALESCE(D.NONPROD_SEATS, 0)                   AS NONPROD_SEATS
                , COALESCE(D.NONPROD_USED, 0)                    AS NONPROD_USED
                , COALESCE(E.SANDBOX_SEATS, 0)                   AS SANDBOX_SEATS
                , COALESCE(E.SANDBOX_USED, 0)                    AS SANDBOX_USED
                , SEATS                                          AS PRIMARY_ROW_SEATS
                , USED_LICENSES                                  AS PRIMARY_ROW_USED 
                , COALESCE(B.LONGEST_INSTALL, MONTHS_INSTALLED)  AS LONGEST_ACTIVE_INSTALL
                , COALESCE(B.LICENSE_ID_LIST, 'None Active')     AS ACTIVE_LICENSE_ID_LIST
                , COALESCE(D.NONPROD_LICENSE_ID_LIST, 'None')    AS NONPROD_LICENSE_ID_LIST
                , COALESCE(E.SANDBOX_LICENSE_ID_LIST, 'None')    AS SANDBOX_LICENSE_ID_LIST   
-- everything below comes from the primary row only
                , STATUS
                , ORG_STATUS 
                , ACCOUNT_ID
                , ACCOUNT_NAME  
                , IS_SANDBOX
                , INSTALL_DATE
                , UNINSTALL_DATE
                , MONTHS_INSTALLED
                , INSTALL_DATE_STRING        
                , EXPIRATION_DATE
                , EXPIRATION_DATE_STRING                           
                , LAST_ACTIVITY_DATE
                , SUSPEND_ACCOUNT_BOOL
                , ACCOUNT_SUSPENDED_REASON
                , LICENSE_NAME
                , C1_PRODUCTION_BOOL
                , A.LICENSE_ID AS PRIMARY_LICENSE_ID
                , PREDICTED_PACKAGE_NAMESPACE                                   
                , CRM_SOURCE
                , L.PRODUCT_LINE -- old c1 product was called product_line
        FROM                          APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT A
        LEFT OUTER JOIN               ACTIVE_LISTS B
                      ON  A.CUSTOMER_ORG = B.CUSTOMER_ORG
                      AND A.PACKAGE_ID = B.PACKAGE_ID
        LEFT OUTER JOIN               ACTIVE_LIC_TYPES C
                      ON  A.CUSTOMER_ORG = C.CUSTOMER_ORG
                      AND A.PACKAGE_ID = C.PACKAGE_ID                             
        LEFT OUTER JOIN               NONPRODUCTION D
                      ON  A.CUSTOMER_ORG = D.CUSTOMER_ORG
                      AND A.PACKAGE_ID = D.PACKAGE_ID
        LEFT OUTER JOIN               SANDBOX E
                      ON  A.CUSTOMER_ORG = E.CUSTOMER_ORG
                      AND A.PACKAGE_ID = E.PACKAGE_ID
        LEFT OUTER JOIN               APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE L
                      ON A.PACKAGE_NAME = L.PACKAGE_NAME                        
        WHERE A.SELECT1_FOR_PACKAGE_ID = 1 -- get the best row for any org and package
         AND (   LAST_ACTIVITY_DATE >= (CURRENT_DATE()-75)
              OR (     LAST_ACTIVITY_DATE < (CURRENT_DATE()-75)
                  AND  UPPER(STATUS) = 'ACTIVE'
                  AND  EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED', 'EXPIRED') 
                  AND  ORG_STATUS NOT IN ('DELETED')
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
    var stepname = "Insert LMA_LIC_PACKAGE_WEEKLY"  
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
			return_value = "Rows Inserted to LMA_LIC_PACKAGE_WEEKLY but duplicate values found"
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
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY(varchar);
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY('Full'); -- the other option is manual     
-- to run in manual mode (this one can only be run or rerun for the recently completed month since it refences the Current views for license 
-- as a future enhancement this could be built to run date driven off of the LMA_LICENSE HISTORY 
-- to rerun for the current month
-- first if they exist already,  delete all records from APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_WEEKLY for the recently completed month
-- <future enhandment only > second run below statement to insert a control record replacing the RUN_FOR_DATE with the month you want to run
--INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, RUN_DATE, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
--SELECT date_trunc('MONTH', CURRENT_DATE()) AS RUN_FOR_MONTH
--     , dateadd(day, -1, CURRENT_DATE()) AS RUN_DATE -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
--     , CURRENT_USER() AS OPERATOR
--     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
--     , 'Incomplete' AS STATUS
--     , 'FILL_LMA_LIC_PACKAGE_WEEKLY' as PROC_OR_STEP
--;
-- then run with Manual mode so it uses the control you just created rather than generating a new one
-- if there are duplicates after your run then something has gone terribly wrong -- figure out which months have duplicates delete and rerun those
-- a delete statement might look something like 
---- --DELETE FROM LMA_LIC_PACKAGE_WEEKLY WHERE REPORT_DATE = '2035-08-01';  
CALL APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY('Manual');

CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 01 02 * * 1 America/Los_Angeles' -- after LMA snapshots
AS CALL APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY('Full')
; 
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY;
alter task APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_LMA_LIC_PACKAGE_WEEKLY resume;

show tasks IN SCHEMA PRODUCT;
show procedures IN SCHEMA PRODUCT;



