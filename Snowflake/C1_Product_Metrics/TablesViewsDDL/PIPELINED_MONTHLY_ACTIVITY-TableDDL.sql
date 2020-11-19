
--DROP TABLE APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY;

CREATE TABLE PIPELINED_MONTHLY_ACTIVITY 
  ( 
     ORG_SOURCE              VARCHAR(255) 
     , SOURCE_ORG_ID         VARCHAR(255) 
     , ACTIVITY_YEAR         NUMBER 
     , ACTIVITY_MONTH        NUMBER 
     , ACTIVITY_MONTH_DATE   DATE 
     , PRODUCT_LINE          VARCHAR(255) 
     , ACTIVITY_COUNT        NUMBER 
     , UNIQUE_USERS          NUMBER 
     , IS_SANDBOX_EDITION    BOOLEAN 
     , ACTIVITY_ACCOUNT_ID   VARCHAR(16777216) 
     , ACTIVITY_ACCOUNT_NAME VARCHAR(16777216) 
  ); 

INSERT INTO APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY
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
--          and Year(REQUEST_TIMESTAMP) = YEAR(dateadd(month, -1, CURRENT_DATE()))
--          and Month(REQUEST_TIMESTAMP) = MONTH(dateadd(month, -1, CURRENT_DATE()))
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
) , setDate as (
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
)
        SELECT A.ORG_SOURCE
             , A.SOURCE_ORG_ID
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , A.ACTIVITY_MONTH_DATE 
             , A.PRODUCT_LINE
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION
             , null as ACTIVITY_ACCOUNT_ID
             , null as ACTIVITY_ACCOUNT_NAME
        FROM setDate A       
        WHERE A.ACTIVITY_MONTH_DATE < '2020-08-05' 
;  

INSERT into APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY
WITH EDIT_SENT_EVENTS AS (
        SELECT INTERNAL_COLLAB_ACCT_ID AS SOURCE_ORG_ID
             , ACCOUNT_NAME
             , ACCOUNT_SF_ID
             , YEAR(DATE) AS ACTIVITY_YEAR
             , MONTH(DATE) AS ACTIVITY_MONTH
             , USER_ID
             , 0::BOOLEAN AS IS_SANDBOX_EDITION                      
        FROM                     APTTUS_DW.SF_PRODUCTION.COLLABORATE_MAU
--        WHERE Year(DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
--          and Month(DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))
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
, setDate as (      
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
)
        SELECT A.ORG_SOURCE
             , A.SOURCE_ORG_ID
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , A.ACTIVITY_MONTH_DATE 
             , A.PRODUCT_LINE
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION
             , A.ACTIVITY_ACCOUNT_ID
             , A.ACTIVITY_ACCOUNT_NAME
        FROM setDate A       
        WHERE A.ACTIVITY_MONTH_DATE < '2020-08-05'                                   
; 

INSERT into APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY
WITH GET_CONTRACT_ACTIVITY AS (
        SELECT USER_COMPANY_UUID
             , Year(REPORT_DATE) AS ACTIVITY_YEAR
             , Month(REPORT_DATE) AS ACTIVITY_MONTH
             , COUNT(DISTINCT CONTRACT_UUID) AS ACTIVITY_COUNT
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_CONTRACT_COUNTS  
--        WHERE Year(REPORT_DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
--          and Month(REPORT_DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))          
        GROUP BY USER_COMPANY_UUID, ACTIVITY_YEAR, ACTIVITY_MONTH        
)  
, GET_CONTRACT_USERS AS (
       SELECT USER_COMPANY_UUID
             , Year(REPORT_DATE) AS ACTIVITY_YEAR
             , Month(REPORT_DATE) AS ACTIVITY_MONTH
             , COUNT(DISTINCT APP_USER_UUID) AS UNIQUE_USERS
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_LOGINS
--        WHERE Year(REPORT_DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
--          and Month(REPORT_DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))          
        GROUP BY USER_COMPANY_UUID, ACTIVITY_YEAR, ACTIVITY_MONTH        
)  
, GET_COMPANY AS (
        SELECT USER_COMPANY_UUID
             , ENVIRONMENT
             , CASE 
                 WHEN UPPER(ENVIRONMENT) LIKE '%SANDBOX%'
                   THEN 1::BOOLEAN
                 WHEN UPPER(ENVIRONMENT) LIKE '%DEMO%'                   
                   THEN 1::BOOLEAN
                ELSE 0::BOOLEAN
               END AS IS_SANDBOX_EDITION 
             , COMPANY_NAME  
        FROM APTTUS_DW.PRODUCT.CONTRACTS_CLIENT_CONFIGURATION_CURRENT -- THIS IS A VIEW
)
, GET_ACTY_USERS AS (
        SELECT COALESCE(A.USER_COMPANY_UUID, B.USER_COMPANY_UUID) AS USER_COMPANY_UUID
             , COALESCE(A.ACTIVITY_YEAR, B.ACTIVITY_YEAR) AS ACTIVITY_YEAR
             , COALESCE(A.ACTIVITY_MONTH, B.ACTIVITY_MONTH) AS ACTIVITY_MONTH
             , COALESCE(A.ACTIVITY_COUNT, 0) AS ACTIVITY_COUNT
             , COALESCE(B.UNIQUE_USERS, 0) AS UNIQUE_USERS
             , C.IS_SANDBOX_EDITION
             , C.COMPANY_NAME           
        FROM                GET_CONTRACT_ACTIVITY A
        FULL OUTER JOIN     GET_CONTRACT_USERS B
                        ON  A.USER_COMPANY_UUID = B.USER_COMPANY_UUID
                        AND A.ACTIVITY_YEAR = B.ACTIVITY_YEAR
                        AND A.ACTIVITY_MONTH = B.ACTIVITY_MONTH
        INNER JOIN          GET_COMPANY C   
                        ON  COALESCE(A.USER_COMPANY_UUID, B.USER_COMPANY_UUID) = C.USER_COMPANY_UUID
)
, setDate as (
        SELECT 'CONTRACTS' AS ORG_SOURCE
             , A.USER_COMPANY_UUID AS SOURCE_ORG_ID 
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE
             , 'Conga Contracts' AS PRODUCT_LINE
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION
             , NULL as ACTIVITY_ACCOUNT_ID
             , A.COMPANY_NAME AS ACTIVITY_ACCOUNT_NAME             
        FROM                    GET_ACTY_USERS A
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1   
)
        SELECT A.ORG_SOURCE
             , A.SOURCE_ORG_ID
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH
             , A.ACTIVITY_MONTH_DATE 
             , A.PRODUCT_LINE
             , A.ACTIVITY_COUNT
             , A.UNIQUE_USERS
             , A.IS_SANDBOX_EDITION
             , A.ACTIVITY_ACCOUNT_ID
             , A.ACTIVITY_ACCOUNT_NAME
        FROM setDate A       
        WHERE A.ACTIVITY_MONTH_DATE < '2020-08-05'                                   
; 

select count(*), PRODUCT_LINE
from APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY
group by PRODUCT_LINE
;

--delete from APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY
where PRODUCT_LINE = 'Conga Contracts'
;