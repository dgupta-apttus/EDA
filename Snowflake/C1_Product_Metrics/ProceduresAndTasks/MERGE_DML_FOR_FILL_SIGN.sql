
MERGE INTO APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY TARGET_T 
using (
WITH EDIT_SENT_EVENTS AS (
        SELECT SYSTEM_TYPE AS ORG_SOURCE
             , CASE
                 WHEN SYSTEM_TYPE <> 'COLLABORATE'
                   THEN SYSTEM_ID 
                 WHEN SYSTEM_TYPE = 'COLLABORATE'
                  AND SYSTEM_ID  LIKE 'collaborate-production-%'
                   THEN SUBSTRING(SYSTEM_ID ,24)
                else SYSTEM_ID         
               END AS SOURCE_ORG_ID
             , YEAR(REQUEST_TIMESTAMP) AS ACTIVITY_YEAR
             , MONTH(REQUEST_TIMESTAMP) AS ACTIVITY_MONTH
             , SENDER_ID
             , CASE 
                 WHEN SYSTEM_ENVIRONMENT <> 'SANDBOX'
                   THEN 0::BOOLEAN
                 ELSE 1::BOOLEAN
               END AS IS_SANDBOX_EDITION                      
        FROM APTTUS_DW.SF_PRODUCTION.SIGN_SIGNINGREQUEST_EVENT 
        WHERE EVENT_TYPE = 'SENT'
          and Year(REQUEST_TIMESTAMP) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(REQUEST_TIMESTAMP) = MONTH(dateadd(month, -1, CURRENT_DATE()))
)        
, GROUP_EVENTS AS (
        SELECT ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , COUNT(*) AS ACTIVITY_COUNT
             , COUNT(DISTINCT SENDER_ID) AS UNIQUE_USERS
             , BOOLAND_AGG(IS_SANDBOX_EDITION) AS IS_SANDBOX_EDITION                      
        FROM EDIT_SENT_EVENTS
        GROUP BY ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH               
)
        SELECT A.ORG_SOURCE
             , A.SOURCE_ORG_ID
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE 
             , 'Conga Sign' as PRODUCT_LINE             
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION    
        FROM                   GROUP_EVENTS A      
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1 
) SOURCE_T
    ON  TARGET_T.ORG_SOURCE = SOURCE_T.ORG_SOURCE
    AND TARGET_T.SOURCE_ORG_ID = SOURCE_T.SOURCE_ORG_ID
    AND TARGET_T.ACTIVITY_MONTH_DATE = SOURCE_T.ACTIVITY_MONTH_DATE
    AND TARGET_T.PRODUCT_LINE = SOURCE_T.PRODUCT_LINE
WHEN NOT MATCHED THEN 
INSERT 
(              ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , ACTIVITY_MONTH_DATE 
             , PRODUCT_LINE
             , ACTIVITY_COUNT
             , UNIQUE_USERS
             , IS_SANDBOX_EDITION
) VALUES
(              SOURCE_T.ORG_SOURCE
             , SOURCE_T.SOURCE_ORG_ID
             , SOURCE_T.ACTIVITY_YEAR
             , SOURCE_T.ACTIVITY_MONTH
             , SOURCE_T.ACTIVITY_MONTH_DATE 
             , SOURCE_T.PRODUCT_LINE
             , SOURCE_T.ACTIVITY_COUNT
             , SOURCE_T.UNIQUE_USERS
             , SOURCE_T.IS_SANDBOX_EDITION 
)                         
