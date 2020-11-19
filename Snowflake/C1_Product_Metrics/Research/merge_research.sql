SELECT ORG_SOURCE, SOURCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH, ACTIVITY_MONTH_DATE, PRODUCT_LINE, ACTIVITY_COUNT, UNIQUE_USERS, LICENSE_ID 
;
select MAX(ACTIVITY_MONTH_DATE)
FROM FMA_MONTHLY_ACTIVITY;

--INSERT INTO FMA_MONTHLY_ACTIVITY 
--(ORG_SOURCE, SOURCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH, ACTIVITY_MONTH_DATE, PRODUCT_LINE, ACTIVITY_COUNT, UNIQUE_USERS, LICENSE_ID)
with step1 as ( 
        SELECT SOURCE_ORG_ID
             , ACTIVITY_DATE 
             , PRODUCT_LINE
             , LICENSE_ID
             , MAX(ROLLING_ACTIVITY_COUNT) AS ACTIVITY_COUNT
             , MAX(ROLLING_ACTIVE_USERS) AS UNIQUE_USERS
             , SUM(CONTRACTS4SF_DAILY_ACTIVITY) AS ACTIVITY2
        FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
        where ACTIVITY_DATE = CURRENT_DATE()
        group by SOURCE_ORG_ID
             , Year(ACTIVITY_DATE)
             , Month(ACTIVITY_DATE)
             , PRODUCT_LINE 
             , LICENSE_ID  
)
        SELECT 'SALESFORCE' AS ORG_SOURCE
             , SOURCE_ORG_ID
             , ACTIVITY_DATE 
--             , B."Date" as ACTIVITY_MONTH_DATE             
             , PRODUCT_LINE
             , CASE 
                 WHEN PRODUCT_LINE = 'Conga Contracts for Salesforce'
                   THEN CONTRACTS4SF_DAILY_ACTIVITY
                ELSE ROLLING_ACTIVITY_COUNT
               END AS ACTIVITY_COUNT
             , ROLLING_ACTIVE_USERS as UNIQUE_USERS                   
             , LICENSE_ID
             -- get sandbox or non production from license later
        FROM                   step1 A
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1   
;                                

with inner1 as (
        SELECT SOURCE_ORG_ID
             , ACTIVITY_DATE 
             , PRODUCT_LINE
             , LICENSE_ID
             , CASE 
                 WHEN PRODUCT_LINE = 'Conga Contracts for Salesforce'
                   THEN CONTRACTS4SF_DAILY_ACTIVITY
                ELSE ROLLING_ACTIVITY_COUNT
               END AS ACTIVITY_COUNT
             , ROLLING_ACTIVE_USERS as UNIQUE_USERS 
        FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
        where ACTIVITY_DATE = CURRENT_DATE()
)
select count(*), SOURCE_ORG_ID
             , ACTIVITY_DATE 
             , PRODUCT_LINE
             , LICENSE_ID
from inner1
group by SOURCE_ORG_ID
             , ACTIVITY_DATE 
             , PRODUCT_LINE
             , LICENSE_ID
having count(*) > 1
;  
SELECT environment_id, source_org_id, org_source, account_name, salesforce_environment_org_id, collaborate_account_id, contracts_client_id, salesforce_account_id 
FROM env_keys
where source_org_id not like '00D%'
;



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
        group by SALESFORCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH
)
, setDate as (
        SELECT 'SALESFORCE' AS ORG_SOURCE
             , SALESFORCE_ORG_ID AS SOURCE_ORG_ID
             , ACTIVITY_YEAR
             , ACTIVITY_MONTH
             , B."Date" as ACTIVITY_MONTH_DATE             
             , 'Conga Composer' as PRODUCT_LINE
             , ACTIVITY_COUNT
             , UNIQUE_USERS                   
             , A.IS_SANDBOX_EDITION  
             , PACKAGE_NAMESPACE
             , ACTIVITY_ACCOUNT_ID
             , ACTIVITY_ACCOUNT_NAME
        FROM                   get_merges A
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1 
)                  
        SELECT ORG_SOURCE
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
        FROM setDate
        WHERE ACTIVITY_MONTH_DATE < '2020-08-05'                             
;


select *
from get_merges
where SALESFORCE_ORG_ID IN ('00DA0000000a98QMAQ','00D7E0000000lFpUAI','00D7E0000000pIaUAI','00Dw0000000D2YmEAK')
;
select count(distinct ACCOUNT_ID)
      , SALESFORCE_ORG_ID
from get_merges
--where SALESFORCE_ORG_ID not like '00D%'
group by SALESFORCE_ORG_ID
having count(distinct ACCOUNT_ID) > 1
;

select *
from APTTUS_DW.SF_PRODUCTION.COMPOSER_MERGE_EVENT_LOAD
where ACCOUNT_ID not like '001%'
;


select ACCOUNTID_18__C
     , ACCOUNTNUMBER
     , SALESFORCE_ACCOUNT_ID__C
FROM APTTUS_DW.SF_CONGA1_0.ACCOUNT
--WHERE SALESFORCE_ACCOUNT_ID__C like 'a%'
;

        select M.environment_id
             , E.org_source
             , E.source_org_id 
             , coalesce(E.salesforce_environment_org_id, 'see source org') as salesforce_org_id
             , M.activity_year
             , M.activity_month
             , D.date_id as activity_month_date_id
             , D.this_date as activity_month_date
             , 'Conga Composer' as product_line
             , M.activity_count
             , M.unique_users
             , M.is_sandbox_edition
        from                    get_merges M
        inner join              dim_date D
                      on  M.activity_year = D.year
                      and M.activity_month = D.month
                      and 1 = D.day_of_month      
        INNER JOIN              env_keys E
                      ON M.environment_id = E.environment_id  

;

SELECT count(*), PRODUCT_LINE 
FROM APTTUS_DW.PRODUCT.ACTIVITY_MONTHLY_SUMMARY_RSH
group by PRODUCT_LINE
;

SELECT MAX(ACTIVITY_MONTH_DATE)
FROM APTTUS_DW.PRODUCT.ACTIVITY_MONTHLY_SUMMARY_RSH
where PRODUCT_LINE = 'Conga Composer'
;
