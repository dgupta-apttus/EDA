--DROP TABLE APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY;
CREATE TABLE APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY AS
--INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_TEMP
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
--
, ACTIVE_LIC_TYPES as ( 
        SELECT count(*) as LICENSES_WSEATS
             , CUSTOMER_ORG
             , PRODUCT
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
          AND LICENSE_SEAT_TYPE = 'Seats'
        group by CUSTOMER_ORG, PRODUCT  
)          
, ACTIVE_LISTS AS (
        select    listagg(DISTINCT PACKAGE_NAME, ', ') within group (ORDER BY PACKAGE_NAME) AS PACKAGE_LIST 
                , listagg(DISTINCT PACKAGE_ID, ', ') within group (ORDER BY PACKAGE_ID) AS PACKAGE_ID_LIST
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS LICENSE_ID_LIST
                , CUSTOMER_ORG
                , PRODUCT
                , COUNT(*) AS LICENSE_COUNT
                , COUNT(DISTINCT PACKAGE_ID) AS PACKAGE_COUNT
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
        group by CUSTOMER_ORG, PRODUCT
)
, NONPRODUCTION AS (
        select    CUSTOMER_ORG
                , PRODUCT
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS NONPROD_LICENSE_ID_LIST
                , COUNT(*) AS NONPROD_LICENSE_COUNT
                , SUM(SEATS) as NONPROD_SEATS
                , SUM(USED_LICENSES) AS NONPROD_USED
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              ) 
        group by CUSTOMER_ORG, PRODUCT
)
, SANDBOX AS (
        select    CUSTOMER_ORG
                , PRODUCT
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS SANDBOX_LICENSE_ID_LIST
                , COUNT(*) AS SANDBOX_LICENSE_COUNT
                , SUM(SEATS) as SANDBOX_SEATS
                , SUM(USED_LICENSES) AS SANDBOX_USED
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE IS_SANDBOX = true
        group by CUSTOMER_ORG, PRODUCT
)
, tempInner as (
        select    A.CUSTOMER_ORG  
                , A.PRODUCT 
                , (SELECT COMPLETED_MONTH FROM SET_DATE_RANGE) AS REPORTING_MONTH -- set to previous month
                , A.CUSTOMER_ORG_15
                , A.CUSTOMER_ORG_18  
                , A.PACKAGE_NAME
                , COALESCE(B.PACKAGE_LIST, 'None Active') AS ACTIVE_PACKAGE_LIST
                , A.PACKAGE_ID
                , COALESCE(B.PACKAGE_ID_LIST, 'None Active') AS PACKAGE_ID_LIST
                , COALESCE(B.PACKAGE_COUNT, 0) AS PACKAGE_COUNT
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
                      AND A.PRODUCT = B.PRODUCT
        LEFT OUTER JOIN               ACTIVE_LIC_TYPES C
                      ON  A.CUSTOMER_ORG = C.CUSTOMER_ORG
                      AND A.PRODUCT = C.PRODUCT   
        LEFT OUTER JOIN               NONPRODUCTION D
                      ON  A.CUSTOMER_ORG = D.CUSTOMER_ORG
                      AND A.PRODUCT = D.PRODUCT
        LEFT OUTER JOIN               SANDBOX E
                      ON  A.CUSTOMER_ORG = E.CUSTOMER_ORG
                      AND A.PRODUCT = E.PRODUCT
        LEFT OUTER JOIN               APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE L
                      ON A.PACKAGE_NAME = L.PACKAGE_NAME                                                               
        WHERE A.SELECT1_FOR_PRODUCT = 1
         AND (   LAST_ACTIVITY_DATE >= (CURRENT_DATE()-75)
              OR (     LAST_ACTIVITY_DATE < (CURRENT_DATE()-75)
                  AND  UPPER(STATUS) = 'ACTIVE'
                  AND  EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED', 'EXPIRED') 
                 ) 
             )
)

select * 
from tempInner
 
;  