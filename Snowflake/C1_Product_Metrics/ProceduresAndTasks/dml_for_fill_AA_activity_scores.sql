--INSERT INTO APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY_SCORES
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_MONTHLY_AA_ACTIVITY_SCORES'  -- still need to change this 
)
--
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_MONTHLY_AA_ACTIVITY_SCORES' 
)
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL)) AS COMPLETED_MONTH
             , date_trunc('MONTH', dateadd(month, -3, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P3_START_MONTH  
             , date_trunc('MONTH', dateadd(month, -1, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P3_END_MONTH
             , date_trunc('MONTH', dateadd(month, -11, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS CRY_START_MONTH
             , date_trunc('MONTH', dateadd(month, -23, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS PRY_START_MONTH
             , date_trunc('MONTH', dateadd(month, -12, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS PRY_END_MONTH
             , date_trunc('MONTH', dateadd(month, -12, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P12_START_MONTH
             , date_trunc('MONTH', dateadd(month, -1, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS P12_END_MONTH
             , date_trunc('MONTH', dateadd(month, -5, (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL))) AS LAST_ACT_LIMIT
             , YEAR(COMPLETED_MONTH) AS REPORT_YEAR
             , MONTH(COMPLETED_MONTH) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE
)
, AVERAGES_P3 AS (
        SELECT CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE                        
             , AVG(MONTHLY_ACTIVITY) AS PRIOR3_ACTIVITY
             , AVG(MONTHLY_ACTIVE_USERS) AS PRIOR3_USERS             
             , COUNT(*) AS RECENT_MONTHS_OF_ACTIVITY
        FROM APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
        WHERE REPORT_DATE 
           BETWEEN (SELECT P3_START_MONTH FROM SET_DATE_RANGE) 
               AND (SELECT P3_END_MONTH FROM SET_DATE_RANGE)   
        GROUP BY CRM 
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE   
)
--
, LAST_ACTIVITY AS (
        SELECT CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE 
             , MAX(REPORT_DATE) AS LAST_ACTIVITY_MONTH
             , COUNT(*) AS TOTAL_MONTHS_OF_ACTIVITY
             , MEDIAN(MONTHLY_ACTIVITY) AS HISTORIC_MEDIAN_ACTIVITY
             , MEDIAN(MONTHLY_ACTIVE_USERS) AS HISTORIC_MEDIAN_USERS
        FROM APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
        WHERE REPORT_DATE <= (SELECT COMPLETED_MONTH FROM SET_DATE_RANGE)                    
        GROUP BY CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE 
)
--
, CURRENT_ACTIVITY AS (
        SELECT CRM
                , REPORT_DATE
                , REPORT_YEAR_MONTH
                , ORGANIZATION_ID
                , PACKAGE_ID
                , MANAGED_PACKAGE_NAMESPACE
                , MONTHLY_ACTIVE_USERS 
                , MONTHLY_ACTIVITY             
        FROM APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
        WHERE REPORT_DATE = (SELECT COMPLETED_MONTH FROM SET_DATE_RANGE)
)
--
, CRY AS ( -- CURRENT ROLLING12 MONTH YEAR including most recent month
        SELECT CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE 
             , SUM(MONTHLY_ACTIVITY) AS CRY_ACTIVITY
             , SUM(MONTHLY_ACTIVE_USERS) AS CRY_USERS                      
        FROM APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
        WHERE REPORT_DATE >= (SELECT CRY_START_MONTH FROM SET_DATE_RANGE)   
        GROUP BY CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE          
)
--
, PRY AS ( -- PRIOR ROLLING12 MONTH YEAR
        SELECT CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE 
             , SUM(MONTHLY_ACTIVITY) AS PRY_ACTIVITY
             , SUM(MONTHLY_ACTIVE_USERS) AS PRY_USERS
        FROM APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
        WHERE REPORT_DATE BETWEEN (SELECT PRY_START_MONTH FROM SET_DATE_RANGE) AND (SELECT PRY_END_MONTH FROM SET_DATE_RANGE)   
        GROUP BY CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE            
)
--
, P12 AS ( -- prior 12 months
        SELECT CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE 
             , MAX(MONTHLY_ACTIVITY) AS P12_MAX_ACTIVITY
             , MIN(MONTHLY_ACTIVITY) AS P12_MIN_ACTIVITY  
             , (P12_MAX_ACTIVITY + P12_MIN_ACTIVITY)/2 AS ACTIVITY_RANGE_CENTER 
             , MAX(MONTHLY_ACTIVE_USERS) AS P12_MAX_USERS
             , MIN(MONTHLY_ACTIVE_USERS) AS P12_MIN_USERS                        
             , (P12_MAX_USERS + P12_MIN_USERS)/2 AS USERS_RANGE_CENTER
        FROM APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
        WHERE REPORT_DATE BETWEEN (SELECT P12_START_MONTH FROM SET_DATE_RANGE) AND (SELECT P12_END_MONTH FROM SET_DATE_RANGE)
        GROUP BY CRM
             , ORGANIZATION_ID
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE             
)
--
, JOINS_CALCS AS (
        SELECT L.CRM
             , L.ORGANIZATION_ID
             , D.REPORT_YEAR
             , D.REPORT_MONTH
             , D.REPORT_DATE
             , L.LAST_ACTIVITY_MONTH
             , L.PACKAGE_ID
             , L.MANAGED_PACKAGE_NAMESPACE 
             , COALESCE(A.MONTHLY_ACTIVITY, 0) AS ACTIVITY_COUNT
             , COALESCE(A.MONTHLY_ACTIVITY, 0) AS ACT
             , COALESCE(B.PRIOR3_ACTIVITY, 0) AS PRIOR3_ACTIVITY
             , CASE WHEN B.PRIOR3_ACTIVITY > 0
                 THEN COALESCE((ACT-B.PRIOR3_ACTIVITY)/B.PRIOR3_ACTIVITY, 0)
                    WHEN ACT > 0
                 THEN 1   
                ELSE 0  
               END AS ACTIVITY_P3_INTERVAL
             , CASE WHEN B.PRIOR3_ACTIVITY > 0
                 THEN COALESCE(A.MONTHLY_ACTIVITY/B.PRIOR3_ACTIVITY, 0)
                    WHEN A.MONTHLY_ACTIVITY > 0
                 THEN 1   
                ELSE 0  
               END AS ACTIVITY_P3_INTERVAL2               
             , CASE
                 WHEN ACTIVITY_P3_INTERVAL2 <= 1
                   THEN ACTIVITY_P3_INTERVAL2
                ELSE 1    
               END AS ADOPTION_ACTIVITY_UI -- UNIT INTERVAL IE [0,1]                             
             , CASE  
                  WHEN ACTIVITY_P3_INTERVAL BETWEEN -0.04 AND 0.04 THEN '='
                  WHEN ACTIVITY_P3_INTERVAL > 1 THEN '++'
                  WHEN ACTIVITY_P3_INTERVAL > 0 THEN '+'
                  WHEN ACTIVITY_P3_INTERVAL > -.3 THEN '-'
                ELSE '--'      
               END AS ACTIVITY_DIRECTION
             , L.HISTORIC_MEDIAN_ACTIVITY
             , CASE WHEN L.HISTORIC_MEDIAN_ACTIVITY > 0
                 THEN COALESCE((ACT-L.HISTORIC_MEDIAN_ACTIVITY)/L.HISTORIC_MEDIAN_ACTIVITY, 0)
                ELSE 0  
               END AS HISTORIC_MEDIAN_ACTIVITY_INTERVAL
             , CASE 
                 WHEN HISTORIC_MEDIAN_ACTIVITY_INTERVAL BETWEEN -0.1 AND 0.09 THEN '='
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
                   THEN (CY_ACTIVITY-PY_ACTIVITY)/PY_ACTIVITY
                 WHEN CY_ACTIVITY > PY_ACTIVITY  
                   THEN 0.1
                 WHEN PY_ACTIVITY > CY_ACTIVITY
                   THEN -0.3  
                ELSE 0      
               END AS YOY_ACTIVITY_INTERVAL            
             , CASE 
                 WHEN YOY_ACTIVITY_INTERVAL = 0 THEN '='
                  WHEN YOY_ACTIVITY_INTERVAL > 1 THEN '++'
                  WHEN YOY_ACTIVITY_INTERVAL > 0 THEN '+'
                  WHEN YOY_ACTIVITY_INTERVAL > -0.3 THEN '-'
                ELSE '--'
               END AS YOY_ACTIVITY_DIRECTION  
             , P12.P12_MIN_ACTIVITY
             , P12.P12_MAX_ACTIVITY
             , P12.ACTIVITY_RANGE_CENTER  
             , CASE
                 WHEN (P12.ACTIVITY_RANGE_CENTER IS NULL
                       or (P12.ACTIVITY_RANGE_CENTER - P12.P12_MIN_ACTIVITY) < 1
                      ) and ACT > 0  
                   THEN 1
                 WHEN (P12.ACTIVITY_RANGE_CENTER IS NULL
                       or (P12.ACTIVITY_RANGE_CENTER - P12.P12_MIN_ACTIVITY) < 1
                      ) 
                   THEN 0    
                 WHEN ACT BETWEEN P12_MIN_ACTIVITY AND P12_MAX_ACTIVITY 
                   THEN (ACT - P12.ACTIVITY_RANGE_CENTER)/(P12.ACTIVITY_RANGE_CENTER - P12.P12_MIN_ACTIVITY)
                 WHEN ACT > P12_MAX_ACTIVITY
                   THEN 2
                 WHEN ACT < P12_MIN_ACTIVITY    
                   THEN -2
                ELSE -99
               END AS ACTIVITY_RANGE_SCORE   
-- USERS NOW                     
             , COALESCE(A.MONTHLY_ACTIVE_USERS, 0) AS UNIQUE_USERS
             , COALESCE(A.MONTHLY_ACTIVE_USERS, 0) AS UUSR
             , COALESCE(B.PRIOR3_USERS, 0) AS PRIOR3_USERS
             , CASE WHEN B.PRIOR3_USERS > 0
                 THEN COALESCE((UUSR-B.PRIOR3_USERS)/B.PRIOR3_USERS, 0)
                    WHEN UUSR > 0
                 THEN 1   
                ELSE 0  
               END AS USER_P3_INTERVAL
             , CASE WHEN B.PRIOR3_USERS > 0
                 THEN COALESCE(UUSR/B.PRIOR3_USERS, 0)
                    WHEN UUSR > 0
                 THEN 1   
                ELSE 0  
               END AS USER_P3_INTERVAL2
             , CASE
                 WHEN USER_P3_INTERVAL2 <= 1
                   THEN USER_P3_INTERVAL2
                ELSE 1    
               END AS ADOPTION_USER_UI
             , CASE  
                  WHEN USER_P3_INTERVAL BETWEEN -0.04 AND 0.04 THEN '='
                  WHEN USER_P3_INTERVAL > 1 THEN '++'
                  WHEN USER_P3_INTERVAL > 0 THEN '+'
                  WHEN USER_P3_INTERVAL > -0.3 THEN '-'
                ELSE '--'      
               END AS USER_DIRECTION
             , L.HISTORIC_MEDIAN_USERS
             , CASE WHEN L.HISTORIC_MEDIAN_USERS > 0
                 THEN COALESCE((UUSR-L.HISTORIC_MEDIAN_USERS)/L.HISTORIC_MEDIAN_USERS, 0)
                ELSE 0  
               END AS HISTORIC_MEDIAN_USER_INTERVAL 
             , CASE 
                 WHEN HISTORIC_MEDIAN_USER_INTERVAL BETWEEN -0.1 AND 0.09 THEN '='
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
                   THEN (CY_USERS-PY_USERS)/PY_USERS
                 WHEN CY_USERS > PY_USERS  
                   THEN 0.1
                 WHEN PY_USERS > CY_USERS
                   THEN -0.3  
                ELSE 0      
               END AS YOY_USERS_INTERVAL          
             , CASE 
                 WHEN YOY_USERS_INTERVAL = 0 THEN '='
                  WHEN YOY_USERS_INTERVAL > 1 THEN '++'
                  WHEN YOY_USERS_INTERVAL > 0 THEN '+'
                  WHEN YOY_USERS_INTERVAL > -0.3 THEN '-'
                ELSE '--'
               END AS YOY_USERS_DIRECTION  
             , P12.P12_MIN_USERS
             , P12.P12_MAX_USERS
             , P12.USERS_RANGE_CENTER  
             , CASE
                 WHEN (P12.USERS_RANGE_CENTER IS NULL
                       or (P12.USERS_RANGE_CENTER - P12.P12_MIN_USERS) < 1
                      ) and UUSR > 0              
                   THEN 1
                 WHEN (P12.USERS_RANGE_CENTER IS NULL
                       or (P12.USERS_RANGE_CENTER - P12.P12_MIN_USERS) < 1
                      )
                   THEN 0   
                 WHEN UUSR BETWEEN P12_MIN_USERS AND P12_MAX_USERS 
                   THEN (UUSR - P12.USERS_RANGE_CENTER)/(P12.USERS_RANGE_CENTER - P12.P12_MIN_USERS)
                 WHEN UUSR > P12_MAX_USERS
                   THEN 2
                 WHEN UUSR < P12_MIN_USERS    
                   THEN -2
                ELSE -99 -- this shouldn't happen
               END AS USERS_RANGE_SCORE             
             , L.TOTAL_MONTHS_OF_ACTIVITY            
        FROM                            LAST_ACTIVITY L
        INNER JOIN                      SET_DATE_RANGE D
                         ON 1 = 1 -- WILL ONLY BE ONE ROW SO CARTESIAN TO FORCE JOIN
        LEFT OUTER JOIN                 CURRENT_ACTIVITY A
                         ON  L.PACKAGE_ID = A.PACKAGE_ID
                         AND L.MANAGED_PACKAGE_NAMESPACE = A.MANAGED_PACKAGE_NAMESPACE
                         AND L.ORGANIZATION_ID = A.ORGANIZATION_ID
                         AND D.REPORT_DATE = A.REPORT_DATE
        LEFT OUTER JOIN                 AVERAGES_P3 B
                         ON  L.PACKAGE_ID = B.PACKAGE_ID
                         AND L.MANAGED_PACKAGE_NAMESPACE = B.MANAGED_PACKAGE_NAMESPACE
                         AND L.ORGANIZATION_ID = B.ORGANIZATION_ID
        LEFT OUTER JOIN                 CRY
                         ON  L.PACKAGE_ID = CRY.PACKAGE_ID
                         AND L.MANAGED_PACKAGE_NAMESPACE = CRY.MANAGED_PACKAGE_NAMESPACE
                         AND L.ORGANIZATION_ID = CRY.ORGANIZATION_ID                       
        LEFT OUTER JOIN                 PRY
                         ON  L.PACKAGE_ID = PRY.PACKAGE_ID
                         AND L.MANAGED_PACKAGE_NAMESPACE = PRY.MANAGED_PACKAGE_NAMESPACE
                         AND L.ORGANIZATION_ID = PRY.ORGANIZATION_ID                             
        LEFT OUTER JOIN                 P12
                         ON  L.PACKAGE_ID = P12.PACKAGE_ID
                         AND L.MANAGED_PACKAGE_NAMESPACE = P12.MANAGED_PACKAGE_NAMESPACE
                         AND L.ORGANIZATION_ID = P12.ORGANIZATION_ID                        
        WHERE L.LAST_ACTIVITY_MONTH >= (SELECT LAST_ACT_LIMIT FROM SET_DATE_RANGE)                                                              
)
        SELECT CRM
             , ORGANIZATION_ID  
             , REPORT_YEAR
             , REPORT_MONTH
             , REPORT_DATE
             , LAST_ACTIVITY_MONTH
             , PACKAGE_ID
             , MANAGED_PACKAGE_NAMESPACE
             , ACTIVITY_COUNT
, PRIOR3_ACTIVITY -- delete later              
             , ROUND(ACTIVITY_P3_INTERVAL, 3) as ACTIVITY_P3_INTERVAL
             , ROUND(ADOPTION_ACTIVITY_UI, 3) as ADOPTION_ACTIVITY_UI
             , ACTIVITY_DIRECTION
, HISTORIC_MEDIAN_ACTIVITY -- delete later
             , ROUND(HISTORIC_MEDIAN_ACTIVITY_INTERVAL, 3) as HISTORIC_MEDIAN_ACTIVITY_INTERVAL
             , HISTORIC_ACTIVITY_DIRECTION       
             , CY_ACTIVITY
             , PY_ACTIVITY
             , ROUND(YOY_ACTIVITY_INTERVAL, 3) as YOY_ACTIVITY_INTERVAL       
             , YOY_ACTIVITY_DIRECTION 
, P12_MIN_ACTIVITY -- delete
, P12_MAX_ACTIVITY -- delete
, ACTIVITY_RANGE_CENTER -- delete later              
             , ROUND(ACTIVITY_RANGE_SCORE, 3) as ACTIVITY_RANGE_SCORE         
             , UNIQUE_USERS
, PRIOR3_USERS -- delete later             
             , ROUND(USER_P3_INTERVAL, 3) as USER_P3_INTERVAL
             , ROUND(ADOPTION_USER_UI, 3) as ADOPTION_USER_UI
             , USER_DIRECTION
, HISTORIC_MEDIAN_USERS -- delete later              
             , ROUND(HISTORIC_MEDIAN_USER_INTERVAL, 3) as HISTORIC_MEDIAN_USER_INTERVAL
             , HISTORIC_USER_DIRECTION 
             , CY_USERS
             , PY_USERS
             , ROUND(YOY_USERS_INTERVAL, 3) as YOY_USERS_INTERVAL        
             , YOY_USERS_DIRECTION 
, P12_MIN_USERS -- delete later
, P12_MAX_USERS -- delete later
, USERS_RANGE_CENTER -- delete later             
             , ROUND(USERS_RANGE_SCORE, 3) as USERS_RANGE_SCORE             
             , TOTAL_MONTHS_OF_ACTIVITY
        FROM JOINS_CALCS       

;        