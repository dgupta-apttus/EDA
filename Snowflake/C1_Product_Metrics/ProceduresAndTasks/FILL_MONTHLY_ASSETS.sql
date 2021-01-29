create or replace procedure APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS(MODE VARCHAR)
    returns string
    language javascript
    strict
    as
    $$
    var procname = "FILL_MONTHLY_ASSETS"
    var MODE_INNER = MODE
    var set_run = `
INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
SELECT date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_MONTHLY_ASSETS' as PROC_OR_STEP
` 
    var check_dups = `
select count(*) as DUP_COUNT from (
SELECT COUNT(*) , CRM
       , ASSET_ID 
       , REPORT_DATE 
FROM APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY
GROUP BY CRM
       , ASSET_ID
       , REPORT_DATE 
HAVING COUNT(*) > 1
)
`
    var update_run_complete = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Complete'
WHERE PROC_OR_STEP = 'FILL_MONTHLY_ASSETS'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_MONTHLY_ASSETS')
`
    var update_run_dups = `
UPDATE APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
    SET STATUS = 'Duplicates Created'
WHERE PROC_OR_STEP = 'FILL_MONTHLY_ASSETS'
  AND EXECUTION_TIME = (SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_MONTHLY_ASSETS')
`
    var sql_command = `
INSERT INTO APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY   
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_MONTHLY_ASSETS'
)    
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_MONTHLY_ASSETS' 
                  AND STATUS <> 'Complete'
)
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL)) AS COMPLETED_MONTH
             , YEAR(COMPLETED_MONTH) AS REPORT_YEAR
             , MONTH(COMPLETED_MONTH) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE -- will report on beginning of month
             , date_trunc('MONTH',dateadd(month, 1, COMPLETED_MONTH)) AS LESS_THAN_DATE -- values will represent state at end of month
)          
, get_current_of_C1 AS (
        SELECT MAX(EXTRACT_DATE) AS EXTRACT_DATE, ID
        FROM   APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
        WHERE ACTIVITY_DATE <= (SELECT LESS_THAN_DATE FROM SET_DATE_RANGE)
        group by ID
)
, get_current_of_ALI AS (
        SELECT MAX(EXTRACT_DATE) AS EXTRACT_DATE, ID
        FROM   APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY
        WHERE ACTIVITY_DATE <= (SELECT LESS_THAN_DATE FROM SET_DATE_RANGE)
        group by ID
)
, choose_current_asset as (
        SELECT A.ACCOUNTID
             , 'Conga1.0'                                                   AS CRM
             , A.ID                                                         AS ASSET_ID             
             , A.NAME                                                       AS ASSET_NAME
             , (SELECT REPORT_DATE FROM SET_DATE_RANGE)                     AS REPORT_DATE
             , A.PRODUCT2ID                                                 AS PRODUCT_ID 
             , B.PRODUCT_NAME
             , COALESCE(B.PRODUCT, 'Unidentified Product')                  AS PRODUCT
             , B.PRODUCT_LINE
             , B.PRODUCT_FAMILY
             , B.PRODUCT2_LINE 
             , CASE
                 WHEN B.PRODUCT = 'Conga Contracts'
                   THEN 'CONTRACTS'
                 WHEN B.PRODUCT = 'Conga Collaborate'
                   THEN 'COLLABORATE'
                ELSE 'SALESFORCE'
               END                                                          AS ORG_SOURCE                                
             , CASE
                 WHEN (ENVIRONMENTID__C is NULL
                   OR ENVIRONMENTID__C LIKE '00D%')
                     THEN COALESCE(ORGID18__C, 'Unknown')
                 WHEN B.PRODUCT IN ('Conga Collaborate', 'Conga Contracts')
                     THEN ENVIRONMENTID__C
                ELSE COALESCE(ORGID18__C, 'Unknown')     
               END                                                          AS CUSTOMER_ORG 
             , A.QUANTITY
             , COALESCE((A.MRR_ASSET_MRR__C * 12),0)                        AS ACV   
             , SUM(ACV) OVER (PARTITION BY A.ACCOUNTID)                     AS ACV_ON_ACCOUNT     
             , 'Active'                                                     AS ASSET_STATUS  
             , A.START_DATE__C                                              AS START_DATE 
             , A.END_DATE__C                                                AS END_DATE
             , 'USD'                                                        AS ORIGINAL_CURRENCY
             , ACV                                                          AS ORIGINAL_ACV             
        FROM                     APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY A
        INNER JOIN               get_current_of_C1 D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE           
        LEFT OUTER JOIN         APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_ASSET_MAPPING B
                            ON A.PRODUCT2ID = B.PRODUCT_ID        
        WHERE A.ENTITLEMENT_STATUS__C = 'Active'    
          AND A.TYPE__C = 'Subscription'                 
UNION -- 
        SELECT A.APTTUS_CONFIG2__ACCOUNTID__C                               AS ACCOUNTID
             , 'Apttus1.0'                                                  AS CRM  
             , A.ID                                                         AS ASSET_ID
             , A.NAME                                                       AS ASSET_NAME
             , (SELECT REPORT_DATE FROM SET_DATE_RANGE)                     AS REPORT_DATE
             , A.APTTUS_CONFIG2__PRODUCTID__C                               AS PRODUCT_ID
             , B.PRODUCT_NAME
             , COALESCE(B.PRODUCT, 'Unidentified Product')                  AS PRODUCT
             , B.PRODUCT_LINE
             , B.PRODUCT_FAMILY
             , B.PRODUCT2_LINE 
             , 'SALESFORCE'                                                 AS ORG_SOURCE    
             , 'Unknown'                                                    AS CUSTOMER_ORG               
             , A.APTTUS_CONFIG2__QUANTITY__C                                AS QUANTITY    
             , (COALESCE(A.ACV__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2)      AS ACV
             , SUM(ACV) OVER (PARTITION BY A.APTTUS_CONFIG2__ACCOUNTID__C)  AS ACV_ON_ACCOUNT   
             , 'Active'                                                     AS ASSET_STATUS
             , A.APTTUS_CONFIG2__STARTDATE__C                               AS START_DATE 
             , A.APTTUS_CONFIG2__ENDDATE__C                                 AS END_DATE
             , A.CURRENCYISOCODE                                            AS ORIGINAL_CURRENCY
             , A.ACV__C                                                     AS ORIGINAL_ACV
        FROM                     APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY A
        INNER JOIN               get_current_of_ALI D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE          
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.CURRENCYTYPE  CT -- this is current only
                            ON A.CURRENCYISOCODE = CT.ISOCODE
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_ASSET_MAPPING B
                            ON A.APTTUS_CONFIG2__PRODUCTID__C = B.PRODUCT_ID                                   
        WHERE A.APTTUS_CONFIG2__ASSETSTATUS__C = 'Activated'
          AND (SELECT LESS_THAN_DATE FROM SET_DATE_RANGE) BETWEEN A.APTTUS_CONFIG2__STARTDATE__C AND A.APTTUS_CONFIG2__ENDDATE__C 
)
select * 
from choose_current_asset
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
    var stepname = "Insert MONTHLY_ASSETS"  
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
			return_value = "Rows Inserted to MONTHLY_ASSETS but duplicate values found"
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
       
DESCRIBE procedure APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS(varchar);
--how to call example
CALL APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS('Full'); -- the other option is manual     
-- to run in manual mode
-- first if they exist already delete all records from APTTUS_DW.PRODUCT.MONTHLY_ASSETS that have a month you want to replace or rerun
-- second run below statement to insert a control record replacing the RUN_FOR_DATE with the month you want to run
--INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (PROC_OR_STEP, RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS)
--SELECT 'FILL_MONTHLY_ASSETS' as PROC_OR_STEP
--     ,  date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
--     , CURRENT_USER() AS OPERATOR
--     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
--     , 'Incomplete' AS STATUS
--;
-- then run with Manual mode so it uses the control you just created rather than generating a new one
-- if there are duplicates after your run then something has gone terrible wrong -- figure out which months have duplicates delete and rerun those
-- a delete statement might look something like 
---- --DELETE FROM MONTHLY_ASSETS WHERE REPORT_DATE = '2035-08-01';  
CALL APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS('Manual');

CREATE OR REPLACE TASK APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 38 04 1 * * America/Los_Angeles' -- 1:38 am UTC time on first day of month
AS CALL APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS('Full')
; 
 
DESCRIBE task APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS;
alter task APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS suspend; --resume
alter task APTTUS_DW.PRODUCT.FILL_MONTHLY_ASSETS resume;

show tasks IN SCHEMA PRODUCT;




