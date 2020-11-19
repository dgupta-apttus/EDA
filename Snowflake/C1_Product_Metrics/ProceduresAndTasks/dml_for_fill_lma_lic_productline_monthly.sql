--select count(*) as DUP_COUNT from (
SELECT COUNT(*) , CUSTOMER_ORG_18, PRODUCT_LINE, REPORTING_MONTH 
FROM APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_TEMP
GROUP BY CUSTOMER_ORG_18, PRODUCT_LINE, REPORTING_MONTH 
HAVING COUNT(*) > 1
--)
;

INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
SELECT date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_LMA_LICENSE_MONTHLY' as PROC_OR_STEP
;     

INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_TEMP
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
;  