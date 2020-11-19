WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_MONTHLY_ACTIVITY_SCORES' 
)
--
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_MONTHLY_ACTIVITY_SCORES' 
)
--
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
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , PACKAGE_ID
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
             , PACKAGE_ID
)
--
, LAST_ACTIVITY AS (
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , PACKAGE_ID
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
             , PACKAGE_ID
)
--
, CURRENT_ACTIVITY AS (
        SELECT ORG_SOURCE, SOURCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH, ACTIVITY_MONTH_DATE, PRODUCT_LINE, PACKAGE_ID, ACTIVITY_COUNT, UNIQUE_USERS, IS_SANDBOX_EDITION
             , PACKAGE_NAMESPACE, ACTIVITY_ACCOUNT_ID, ACTIVITY_ACCOUNT_NAME, SERVICE_EVENT_MERGES, PERCENT_SERVICE_EVENTS 
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE = (SELECT COMPLETED_MONTH FROM SET_DATE_RANGE)
)
--
, CRY AS ( -- CURRENT ROLLING12 MONTH YEAR including most recent month
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , PACKAGE_ID
             , SUM(ACTIVITY_COUNT) AS CRY_ACTIVITY
             , SUM(UNIQUE_USERS) AS CRY_USERS                      
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE >= (SELECT CRY_START_MONTH FROM SET_DATE_RANGE)   
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , PACKAGE_ID             
)
, PRY AS ( -- PRIOR ROLLING12 MONTH YEAR
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , PACKAGE_ID
             , SUM(ACTIVITY_COUNT) AS PRY_ACTIVITY
             , SUM(UNIQUE_USERS) AS PRY_USERS
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
        WHERE ACTIVITY_MONTH_DATE BETWEEN (SELECT PRY_START_MONTH FROM SET_DATE_RANGE) AND (SELECT PRY_END_MONTH FROM SET_DATE_RANGE)   
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , PACKAGE_ID             
)
, P12 AS ( -- prior 12 months
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE
             , PACKAGE_ID
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
             , PACKAGE_ID             
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
             , L.PACKAGE_ID
             , COALESCE(A.ACTIVITY_COUNT, 0) AS ACTIVITY_COUNT
             , COALESCE(A.ACTIVITY_COUNT, 0) AS ACT
             , COALESCE(B.PRIOR3_ACTIVITY, 0) AS PRIOR3_ACTIVITY
             , CASE WHEN B.PRIOR3_ACTIVITY > 0
                 THEN COALESCE((A.ACTIVITY_COUNT-B.PRIOR3_ACTIVITY)/B.PRIOR3_ACTIVITY, 0)
                ELSE 0  
               END AS ACTIVITY_P3_INTERVAL
             , CASE WHEN B.PRIOR3_ACTIVITY > 0
                 THEN COALESCE(A.ACTIVITY_COUNT/B.PRIOR3_ACTIVITY, 0)
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
             , COALESCE(A.UNIQUE_USERS, 0) AS UNIQUE_USERS
             , COALESCE(A.UNIQUE_USERS, 0) AS UUSR
             , COALESCE(B.PRIOR3_USERS, 0) AS PRIOR3_USERS
             , CASE WHEN B.PRIOR3_USERS > 0
                 THEN COALESCE((UUSR-B.PRIOR3_USERS)/B.PRIOR3_USERS, 0)
                ELSE 0  
               END AS USER_P3_INTERVAL
             , CASE WHEN B.PRIOR3_USERS > 0
                 THEN COALESCE(UUSR/B.PRIOR3_USERS, 0)
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
                         AND L.PACKAGE_ID = A.PACKAGE_ID
                         AND D.REPORT_DATE = A.ACTIVITY_MONTH_DATE
        LEFT OUTER JOIN                 AVERAGES_P3 B
                         ON  L.ORG_SOURCE = B.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = B.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = B.PRODUCT_LINE
                         AND L.PACKAGE_ID = B.PACKAGE_ID
        LEFT OUTER JOIN                 CRY
                         ON  L.ORG_SOURCE = CRY.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = CRY.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = CRY.PRODUCT_LINE
                         AND L.PACKAGE_ID = CRY.PACKAGE_ID                         
        LEFT OUTER JOIN                 PRY
                         ON  L.ORG_SOURCE = PRY.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = PRY.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = PRY.PRODUCT_LINE
                         AND L.PACKAGE_ID = PRY.PACKAGE_ID                            
        LEFT OUTER JOIN                 P12
                         ON  L.ORG_SOURCE = P12.ORG_SOURCE
                         AND L.SOURCE_ORG_ID = P12.SOURCE_ORG_ID
                         AND L.PRODUCT_LINE = P12.PRODUCT_LINE
                         AND L.PACKAGE_ID = P12.PACKAGE_ID                          
        WHERE LAST_ACTIVITY_MONTH >= (SELECT LAST_ACT_LIMIT FROM SET_DATE_RANGE)                  
-- temp
--and L.SOURCE_ORG_ID IN ('00Di0000000HMOLEA4')
--'00D30000001Fe2REAS','00D1D0000009iaNUAQ','00Dd0000000bansEAA','00DA0000000aShTMAU','00DA0000000IsOXMA0','00D6A000002EtECUA0','00DD0000000lCPDMA2','00D0D0000008eVAUAY','00DG0000000irVuMAI','00D2g0000008d4mEAA','00D1F000000A195UAC','00Dd0000000d1nlEAA','00D17000000DNfvEAG','00Dm00000009EUQEA2','00D0C0000008atcUAA','00D300000000LMqEAM','00D9E0000004iVRUAY','00D5I000003egW3UAI','00D28000001z9tdEAA')                                            
)
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , REPORT_YEAR
             , REPORT_MONTH
             , REPORT_DATE
             , LAST_ACTIVITY_MONTH
             , PRODUCT_LINE
             , PACKAGE_ID
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
             , IS_SANDBOX_EDITION
             , PACKAGE_NAMESPACE
             , ACTIVITY_ACCOUNT_ID
             , ACTIVITY_ACCOUNT_NAME
             , SERVICE_EVENT_MERGES
             , ROUND(PERCENT_SERVICE_EVENTS, 3) as PERCENT_SERVICE_EVENTS
             , TOTAL_MONTHS_OF_ACTIVITY
        FROM JOINS_CALCS   
        
        
        
-- testing criteria
--WHERE ACTIVITY_DIRECTION = '+'
--  AND  TOTAL_MONTHS_OF_ACTIVITY < 5
--  AND IS_SANDBOX_EDITION = false
--WHERE YOY_ACTIVITY_DIRECTION = '+'
--  AND ACTIVITY_DIRECTION = '-'
--  AND  TOTAL_MONTHS_OF_ACTIVITY > 22 
--where SOURCE_ORG_ID = '00DA0000000Kz0lMAC'
--WHERE PY_ACTIVITY > 10  
--  AND  TOTAL_MONTHS_OF_ACTIVITY > 22     
--order by CY_ACTIVITY ASC      
--WHERE ACTIVITY_COUNT = 0
--  AND CY_ACTIVITY > PY_ACTIVITY        
--  AND  TOTAL_MONTHS_OF_ACTIVITY > 22