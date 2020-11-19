INSERT INTO APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY_SCORES
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
)
--
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
)
--
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', dateadd(month, -1, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS COMPLETED_MONTH
             , date_trunc('MONTH', dateadd(month, -4, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P3_START_MONTH  
             , date_trunc('MONTH', dateadd(month, -2, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P3_END_MONTH
              , date_trunc('MONTH', dateadd(month, -12, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS CRY_START_MONTH
             , date_trunc('MONTH', dateadd(month, -24, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS PRY_START_MONTH
             , date_trunc('MONTH', dateadd(month, -13, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS PRY_END_MONTH
             , date_trunc('MONTH', dateadd(month, -13, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P12_START_MONTH
             , date_trunc('MONTH', dateadd(month, -2, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P12_END_MONTH
             , date_trunc('MONTH', dateadd(month, -6, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS LAST_ACT_LIMIT
             , YEAR(dateadd(month, -1, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS REPORT_YEAR
             , MONTH(dateadd(month, -1, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE
)
--
, AVERAGES_P3 AS (
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , AVG(ACTIVITY_COUNT) AS PRIOR3_ACTIVITY
             , AVG(UNIQUE_USERS) AS PRIOR3_USERS             
             , AVG(SERVICE_EVENT_MERGES) AS PRIOR3_AUTOMATION
             , COUNT(*) AS RECENT_MONTHS_OF_ACTIVITY
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE 
           BETWEEN (SELECT P3_START_MONTH FROM SET_DATE_RANGE) 
               AND (SELECT P3_END_MONTH FROM SET_DATE_RANGE)
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
)
--
, LAST_ACTIVITY AS (
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , MAX(ACTIVITY_MONTH_DATE) AS LAST_ACTIVITY_MONTH
             , COUNT(*) AS TOTAL_MONTHS_OF_ACTIVITY
             , BOOLAND_AGG(IS_SANDBOX_EDITION) as IS_SANDBOX_EDITION
             , MAX(PACKAGE_NAMESPACE) AS PACKAGE_NAMESPACE
             , MAX(ACTIVITY_ACCOUNT_ID) AS ACTIVITY_ACCOUNT_ID
             , MAX(ACTIVITY_ACCOUNT_NAME) AS ACTIVITY_ACCOUNT_NAME
             , MEDIAN(ACTIVITY_COUNT) AS HISTORIC_MEDIAN_ACTIVITY
             , MEDIAN(UNIQUE_USERS) AS HISTORIC_MEDIAN_USERS
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE <= (SELECT COMPLETED_MONTH FROM SET_DATE_RANGE)                    
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
)
--
, CURRENT_ACTIVITY AS (
        SELECT ORG_SOURCE, SOURCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH, ACTIVITY_MONTH_DATE, PRODUCT_LINE, ACTIVITY_COUNT, UNIQUE_USERS, IS_SANDBOX_EDITION
             , PACKAGE_NAMESPACE, ACTIVITY_ACCOUNT_ID, ACTIVITY_ACCOUNT_NAME, SERVICE_EVENT_MERGES, PERCENT_SERVICE_EVENTS 
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE = (SELECT COMPLETED_MONTH FROM SET_DATE_RANGE)
)
--
, CRY AS ( -- CURRENT ROLLING12 MONTH YEAR including most recent month
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , SUM(ACTIVITY_COUNT) AS CRY_ACTIVITY
             , SUM(UNIQUE_USERS) AS CRY_USERS                      
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE >= (SELECT CRY_START_MONTH FROM SET_DATE_RANGE)   
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE             
)
, PRY AS ( -- PRIOR ROLLING12 MONTH YEAR
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , SUM(ACTIVITY_COUNT) AS PRY_ACTIVITY
             , SUM(UNIQUE_USERS) AS PRY_USERS
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE BETWEEN (SELECT PRY_START_MONTH FROM SET_DATE_RANGE) AND (SELECT PRY_END_MONTH FROM SET_DATE_RANGE)   
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE             
)
--
, P12 AS ( -- prior 12 months
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , MAX(ACTIVITY_COUNT) AS P12_MAX_ACTIVITY
             , MIN(ACTIVITY_COUNT) AS P12_MIN_ACTIVITY  
             , (P12_MAX_ACTIVITY + P12_MIN_ACTIVITY)/2 AS ACTIVITY_RANGE_CENTER 
             , MAX(UNIQUE_USERS) AS P12_MAX_USERS
             , MIN(UNIQUE_USERS) AS P12_MIN_USERS                        
             , (P12_MAX_USERS + P12_MIN_USERS)/2 AS USERS_RANGE_CENTER
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE BETWEEN (SELECT P12_START_MONTH FROM SET_DATE_RANGE) AND (SELECT P12_END_MONTH FROM SET_DATE_RANGE)
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE             
)
--
, JOINS_CALCS AS (
        SELECT L.ORG_SOURCE
             , L.SOURCE_ORG_ID
             , D.REPORT_YEAR
             , D.REPORT_MONTH
             , D.REPORT_DATE
             , L.LAST_ACTIVITY_MONTH
             , L.PRODUCT_LINE
             , COALESCE(A.ACTIVITY_COUNT, 0) AS ACTIVITY_COUNT
             , COALESCE(A.ACTIVITY_COUNT, 0) AS ACT
             , COALESCE(B.PRIOR3_ACTIVITY, 0) AS PRIOR3_ACTIVITY
             , CASE WHEN B.PRIOR3_ACTIVITY > 0
                 THEN COALESCE(A.ACTIVITY_COUNT/B.PRIOR3_ACTIVITY, 0)
                ELSE 0  
               END AS ACTIVITY_P3_INTERVAL
             , CASE
                 WHEN ACTIVITY_P3_INTERVAL <= 1
                   THEN ACTIVITY_P3_INTERVAL
                ELSE 1    
               END AS ADOPTION_ACTIVITY_UI -- UNIT INTERVAL IE [0,1]
             , CASE  
                  WHEN ACTIVITY_P3_INTERVAL BETWEEN 0.95 AND 1.04 THEN '='
                  WHEN ACTIVITY_P3_INTERVAL > 2 THEN '++'
                  WHEN ACTIVITY_P3_INTERVAL > 1 THEN '+'
                  WHEN ACTIVITY_P3_INTERVAL > .7 THEN '-'
                ELSE '--'      
               END AS ACTIVITY_DIRECTION
             , L.HISTORIC_MEDIAN_ACTIVITY
             , CASE WHEN L.HISTORIC_MEDIAN_ACTIVITY > 0
                 THEN COALESCE(ACT/L.HISTORIC_MEDIAN_ACTIVITY, 0)
                ELSE 0  
               END AS HISTORIC_MEDIAN_ACTIVITY_INTERVAL
             , CASE 
                 WHEN HISTORIC_MEDIAN_ACTIVITY_INTERVAL BETWEEN 0.9 AND 1.09 THEN '='
                 WHEN ACT >= L.HISTORIC_MEDIAN_ACTIVITY
                   THEN '+'
                 WHEN  ACT > 0
                   THEN '-'
                ELSE '--'     
               END AS HISTORIC_ACTIVITY_DIRECTION       
             , COALESCE(CRY.CRY_ACTIVITY, 0) AS CY_ACTIVITY
             , COALESCE(PRY.PRY_ACTIVITY, 0) AS PY_ACTIVITY
             , CASE
                 WHEN PY_ACTIVITY > 0 AND CY_ACTIVITY > 0 AND L.TOTAL_MONTHS_OF_ACTIVITY > 22
                   THEN CY_ACTIVITY/PY_ACTIVITY
                 WHEN CY_ACTIVITY > PY_ACTIVITY  
                   THEN 1
                ELSE 0      
               END AS YOY_ACTIVITY_INTERVAL         
             , CASE 
                 WHEN YOY_ACTIVITY_INTERVAL = 1 THEN '='
                  WHEN YOY_ACTIVITY_INTERVAL > 2 THEN '++'
                  WHEN YOY_ACTIVITY_INTERVAL > 1 THEN '+'
                  WHEN YOY_ACTIVITY_INTERVAL > .7 THEN '-'
                ELSE '--'
               END AS YOY_ACTIVITY_DIRECTION 
             , P12.P12_MIN_ACTIVITY
             , P12.P12_MAX_ACTIVITY
             , P12.ACTIVITY_RANGE_CENTER  
             , CASE
                 WHEN P12.ACTIVITY_RANGE_CENTER IS NULL
                   or P12.ACTIVITY_RANGE_CENTER = 0
                   THEN 2
                 WHEN ACT BETWEEN P12_MIN_ACTIVITY AND P12_MAX_ACTIVITY 
                   THEN 1 + (ACT/P12.ACTIVITY_RANGE_CENTER)
                 WHEN ACT > P12_MAX_ACTIVITY
                   THEN LEAST(4, 1 + (ACT/P12.ACTIVITY_RANGE_CENTER))
                 WHEN ACT < P12_MIN_ACTIVITY    
                   THEN GREATEST(0, (ACT/P12.ACTIVITY_RANGE_CENTER))
                ELSE -1
               END AS ACTIVITY_RANGE_SCORE   
-- USERS NOW                     
             , COALESCE(A.UNIQUE_USERS, 0) AS UNIQUE_USERS
             , COALESCE(A.UNIQUE_USERS, 0) AS UUSR
             , COALESCE(B.PRIOR3_USERS, 0) AS PRIOR3_USERS
             , CASE WHEN B.PRIOR3_USERS > 0
                 THEN COALESCE(UUSR/B.PRIOR3_USERS, 0)
                ELSE 0  
               END AS USER_P3_INTERVAL
             , CASE
                 WHEN USER_P3_INTERVAL <= 1
                   THEN USER_P3_INTERVAL
                ELSE 1    
               END AS ADOPTION_USER_UI
             , CASE  
                  WHEN USER_P3_INTERVAL BETWEEN 0.95 AND 1.04 THEN '='
                  WHEN USER_P3_INTERVAL > 2 THEN '++'
                  WHEN USER_P3_INTERVAL > 1 THEN '+'
                  WHEN USER_P3_INTERVAL > .7 THEN '-'
                ELSE '--'      
               END AS USER_DIRECTION
             , L.HISTORIC_MEDIAN_USERS
             , CASE WHEN L.HISTORIC_MEDIAN_USERS > 0
                 THEN COALESCE(UUSR/L.HISTORIC_MEDIAN_USERS, 0)
                ELSE 0  
               END AS HISTORIC_MEDIAN_USER_INTERVAL 
             , CASE 
                 WHEN HISTORIC_MEDIAN_USER_INTERVAL BETWEEN 0.9 AND 1.09 THEN '='
                 WHEN UUSR >= L.HISTORIC_MEDIAN_USERS
                   THEN '+'
                 WHEN  UUSR > 0
                   THEN '-'
                ELSE '--'     
               END AS HISTORIC_USER_DIRECTION 
             , COALESCE(CRY.CRY_USERS, 0) AS CY_USERS
             , COALESCE(PRY.PRY_USERS, 0) AS PY_USERS
             , CASE
                 WHEN PY_USERS > 0 AND CY_USERS > 0 AND L.TOTAL_MONTHS_OF_ACTIVITY > 22
                   THEN CY_USERS/PY_USERS
                 WHEN CY_USERS > PY_USERS  
                   THEN 1
                ELSE 0      
               END AS YOY_USERS_INTERVAL         
             , CASE 
                 WHEN YOY_USERS_INTERVAL = 1 THEN '='
                  WHEN YOY_USERS_INTERVAL > 2 THEN '++'
                  WHEN YOY_USERS_INTERVAL > 1 THEN '+'
                  WHEN YOY_USERS_INTERVAL > .7 THEN '-'
                ELSE '--'
               END AS YOY_USERS_DIRECTION 
             , P12.P12_MIN_USERS
             , P12.P12_MAX_USERS
             , P12.USERS_RANGE_CENTER  
             , CASE
                 WHEN P12.USERS_RANGE_CENTER IS NULL
                   or P12.USERS_RANGE_CENTER = 0                 
                   THEN 2
                 WHEN UUSR BETWEEN P12_MIN_USERS AND P12_MAX_USERS 
                   THEN 1 + (UUSR/P12.USERS_RANGE_CENTER)
                 WHEN UUSR > P12_MAX_USERS
                   THEN LEAST(4, 1 + (UUSR/P12.USERS_RANGE_CENTER))
                 WHEN UUSR < P12_MIN_USERS    
                   THEN GREATEST(0, (UUSR/P12.USERS_RANGE_CENTER))
                ELSE -1
               END AS USERS_RANGE_SCORE            
             , COALESCE(A.IS_SANDBOX_EDITION, L.IS_SANDBOX_EDITION) AS IS_SANDBOX_EDITION
             , COALESCE(A.PACKAGE_NAMESPACE, L.PACKAGE_NAMESPACE) AS PACKAGE_NAMESPACE
             , COALESCE(A.ACTIVITY_ACCOUNT_ID, L.ACTIVITY_ACCOUNT_ID) AS ACTIVITY_ACCOUNT_ID
             , COALESCE(A.ACTIVITY_ACCOUNT_NAME, L.ACTIVITY_ACCOUNT_NAME) AS ACTIVITY_ACCOUNT_NAME
             , COALESCE(A.SERVICE_EVENT_MERGES, 0) AS SERVICE_EVENT_MERGES
             , COALESCE(A.PERCENT_SERVICE_EVENTS, 0) AS PERCENT_SERVICE_EVENTS
             , L.TOTAL_MONTHS_OF_ACTIVITY            
        FROM                            LAST_ACTIVITY L
        INNER JOIN                      SET_DATE_RANGE D
                         ON 1 = 1 -- WILL ONLY BE ONE ROW SO CARTESIAN TO FORCE JOIN
        LEFT OUTER JOIN                 CURRENT_ACTIVITY A
                         ON  L.ORG_SOURCE = A.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = A.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = A.PRODUCT_LINE
                         AND D.REPORT_DATE = A.ACTIVITY_MONTH_DATE
        LEFT OUTER JOIN                 AVERAGES_P3 B
                         ON  L.ORG_SOURCE = B.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = B.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = B.PRODUCT_LINE
        LEFT OUTER JOIN                 CRY
                         ON  L.ORG_SOURCE = CRY.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = CRY.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = CRY.PRODUCT_LINE                         
        LEFT OUTER JOIN                 PRY
                         ON  L.ORG_SOURCE = PRY.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = PRY.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = PRY.PRODUCT_LINE                            
        LEFT OUTER JOIN                 P12
                         ON  L.ORG_SOURCE = P12.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = P12.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = P12.PRODUCT_LINE                          
        WHERE LAST_ACTIVITY_MONTH >= (SELECT LAST_ACT_LIMIT FROM SET_DATE_RANGE)                                                              
)
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , REPORT_YEAR
             , REPORT_MONTH
             , REPORT_DATE
             , LAST_ACTIVITY_MONTH
             , PRODUCT_LINE
             , ACTIVITY_COUNT
             , ACTIVITY_P3_INTERVAL
             , ADOPTION_ACTIVITY_UI
             , ACTIVITY_DIRECTION
             , HISTORIC_MEDIAN_ACTIVITY_INTERVAL
             , HISTORIC_ACTIVITY_DIRECTION       
             , CY_ACTIVITY
             , PY_ACTIVITY
             , YOY_ACTIVITY_INTERVAL        
             , YOY_ACTIVITY_DIRECTION 
             , ACTIVITY_RANGE_SCORE         
             , UNIQUE_USERS
             , USER_P3_INTERVAL
             , ADOPTION_USER_UI
             , USER_DIRECTION
             , HISTORIC_MEDIAN_USER_INTERVAL 
             , HISTORIC_USER_DIRECTION 
             , CY_USERS
             , PY_USERS
             , YOY_USERS_INTERVAL         
             , YOY_USERS_DIRECTION 
             , USERS_RANGE_SCORE            
             , IS_SANDBOX_EDITION
             , PACKAGE_NAMESPACE
             , ACTIVITY_ACCOUNT_ID
             , ACTIVITY_ACCOUNT_NAME
             , SERVICE_EVENT_MERGES
             , PERCENT_SERVICE_EVENTS
             , TOTAL_MONTHS_OF_ACTIVITY
        FROM JOINS_CALCS              
;
