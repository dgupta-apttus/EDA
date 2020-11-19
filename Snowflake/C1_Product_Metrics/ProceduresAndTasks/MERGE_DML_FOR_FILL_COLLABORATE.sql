
MERGE INTO APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY TARGET_T 
using (
WITH EDIT_SENT_EVENTS AS (
        SELECT INTERNAL_COLLAB_ACCT_ID AS SOURCE_ORG_ID
             , ACCOUNT_NAME
             , ACCOUNT_SF_ID
             , YEAR(DATE) AS ACTIVITY_YEAR
             , MONTH(DATE) AS ACTIVITY_MONTH
             , USER_ID
             , 0::BOOLEAN AS IS_SANDBOX_EDITION                      
        FROM                     APTTUS_DW.SF_PRODUCTION.COLLABORATE_MAU
        WHERE Year(DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))
)        
, GROUP_EVENTS AS (
        SELECT SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , COUNT(*) AS ACTIVITY_COUNT
             , COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
             , BOOLAND_AGG(IS_SANDBOX_EDITION) AS IS_SANDBOX_EDITION
             , MAX(ACCOUNT_NAME) AS ACCOUNT_NAME
             , MAX(ACCOUNT_SF_ID) AS ACCOUNT_SF_ID                     
        FROM EDIT_SENT_EVENTS
        GROUP BY SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH               
)      
        SELECT 'COLLABORATE' AS ORG_SOURCE
             , A.SOURCE_ORG_ID 
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE
             , 'Conga Collaborate' AS PRODUCT_LINE
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION
             , A.ACCOUNT_SF_ID AS ACTIVITY_ACCOUNT_ID
             , A.ACCOUNT_NAME AS ACTIVITY_ACCOUNT_NAME             
        FROM                    GROUP_EVENTS A
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
             , ACTIVITY_ACCOUNT_ID
             , ACTIVITY_ACCOUNT_NAME
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
             , SOURCE_T.ACTIVITY_ACCOUNT_ID
             , SOURCE_T.ACTIVITY_ACCOUNT_NAME 
) 
;                      


select distinct PRODUCT_LINE
from APTTUS_DW.PRODUCT.ACTIVITY_MONTHLY_SUMMARY_RSH
;