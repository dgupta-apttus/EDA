
MERGE INTO APTTUS_DW.PRODUCT.COMPOSER_MONTHLY_ACTIVITY TARGET_T 
using (
with get_merges as (
        select SALESFORCE_ORG_ID 
             , MIN(ACCOUNT_ID) AS ACTIVITY_ACCOUNT_ID
             , MAX(ACCOUNT_NAME) AS ACTIVITY_ACCOUNT_NAME
             , MAX(PACKAGE_NAMESPACE) AS PACKAGE_NAMESPACE
             , Year(MERGE_TIMESTAMP) AS ACTIVITY_YEAR
             , Month(MERGE_TIMESTAMP) AS ACTIVITY_MONTH        
             , count(*) as ACTIVITY_COUNT
             , count(distinct 
                              case 
                                when VERSION_SIMPLE= 'Composer 7' 
                                  then CONTACT_ID 
                                else USER_ID
                              end
                 ) as UNIQUE_USERS
             , BOOLAND_AGG(IS_SANDBOX_EDITION) as IS_SANDBOX_EDITION -- If all values in a set are true, the BOOL_AND function returns true (t). If any value is false, the function returns false (f).
        from APTTUS_DW.SF_PRODUCTION.COMPOSER_MERGE_EVENT_LOAD   
        where Year(MERGE_TIMESTAMP) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(MERGE_TIMESTAMP) = MONTH(dateadd(month, -1, CURRENT_DATE()))
        group by SALESFORCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH
)
, service_event_merges as ( 
        SELECT SALESFORCE_ORG_ID 
             , Year(MERGE_TIMESTAMP) AS ACTIVITY_YEAR
             , Month(MERGE_TIMESTAMP) AS ACTIVITY_MONTH    
             , count(*) as SERVICE_EVENT_MERGES
        FROM APTTUS_DW.SF_PRODUCTION.COMPOSER_MERGE_EVENT_LOAD     
        WHERE  event_type in ('Conductor','Workflow')
          AND Year(MERGE_TIMESTAMP) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          AND Month(MERGE_TIMESTAMP) = MONTH(dateadd(month, -1, CURRENT_DATE()))                                    
        group by SALESFORCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH
)
        SELECT 'SALESFORCE' AS ORG_SOURCE
             , A.SALESFORCE_ORG_ID AS SOURCE_ORG_ID
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE             
             , 'Conga Composer' as PRODUCT_LINE
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS                   
             , A.IS_SANDBOX_EDITION  
             , A.PACKAGE_NAMESPACE
             , A.ACTIVITY_ACCOUNT_ID
             , A.ACTIVITY_ACCOUNT_NAME
             , COALESCE(C.SERVICE_EVENT_MERGES, 0) as SERVICE_EVENT_MERGES
             , CASE
                WHEN C.SERVICE_EVENT_MERGES is null
                   THEN 0
                WHEN A.ACTIVITY_COUNT > 0
                   THEN (C.SERVICE_EVENT_MERGES*100/A.ACTIVITY_COUNT)
                else 0
               end as PERCENT_SERVICE_EVENTS
        FROM                   get_merges A
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1 
        LEFT OUTER JOIN        service_event_merges C
                     ON  A.SALESFORCE_ORG_ID = C.SALESFORCE_ORG_ID
                     AND A.ACTIVITY_YEAR = C.ACTIVITY_YEAR
                     AND A.ACTIVITY_MONTH = C.ACTIVITY_MONTH  
) SOURCE_T
    ON  TARGET_T.ORG_SOURCE = SOURCE_T.ORG_SOURCE
    AND TARGET_T.SOURCE_ORG_ID = SOURCE_T.SOURCE_ORG_ID
    AND TARGET_T.ACTIVITY_MONTH_DATE = SOURCE_T.ACTIVITY_MONTH_DATE
    AND TARGET_T.PRODUCT_LINE = SOURCE_T.PRODUCT_LINE
WHEN NOT MATCHED THEN 
INSERT 
(   ORG_SOURCE
  , SOURCE_ORG_ID
  , ACTIVITY_YEAR
  , ACTIVITY_MONTH
  , ACTIVITY_MONTH_DATE             
  , PRODUCT_LINE
  , ACTIVITY_COUNT
  , UNIQUE_USERS                   
  , IS_SANDBOX_EDITION  
  , PACKAGE_NAMESPACE
  , ACTIVITY_ACCOUNT_ID
  , ACTIVITY_ACCOUNT_NAME
  , SERVICE_EVENT_MERGES
  , PERCENT_SERVICE_EVENTS
) VALUES ( 
    SOURCE_T.ORG_SOURCE
  , SOURCE_T.SOURCE_ORG_ID
  , SOURCE_T.ACTIVITY_YEAR
  , SOURCE_T.ACTIVITY_MONTH
  , SOURCE_T.ACTIVITY_MONTH_DATE             
  , SOURCE_T.PRODUCT_LINE
  , SOURCE_T.ACTIVITY_COUNT
  , SOURCE_T.UNIQUE_USERS                   
  , SOURCE_T.IS_SANDBOX_EDITION  
  , SOURCE_T.PACKAGE_NAMESPACE
  , SOURCE_T.ACTIVITY_ACCOUNT_ID
  , SOURCE_T.ACTIVITY_ACCOUNT_NAME
  , SOURCE_T.SERVICE_EVENT_MERGES
  , SOURCE_T.PERCENT_SERVICE_EVENTS
)          