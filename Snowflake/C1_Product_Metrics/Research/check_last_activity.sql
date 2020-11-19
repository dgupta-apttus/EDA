SELECT count(*) , to_date(REQUEST_TIMESTAMP), REQUEST_REGION
FROM SIGN_SIGNINGREQUEST_EVENT
group by to_date(REQUEST_TIMESTAMP), REQUEST_REGION
order by 2 desc, 3
;

with union_it as (
        select 'CONTRACTS_CLIENT_CONFIGURATION' as load_object
              , MAX(REPORT_DATE) as REPORT_DATE
              , ENVIRONMENT
              , REGION
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_CONFIGURATION
        GROUP BY ENVIRONMENT, REGION
        union
        select 'CONTRACTS_CLIENT_CONTRACT_COUNTS' as load_object
              , MAX(REPORT_DATE) as REPORT_DATE
              , ENVIRONMENT
              , REGION
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_CONTRACT_COUNTS
        GROUP BY ENVIRONMENT, REGION
        union
        select 'CONTRACTS_CLIENT_LOGINS' as load_object
              , MAX(REPORT_DATE) as REPORT_DATE
              , ENVIRONMENT
              , REGION
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_LOGINS
        GROUP BY ENVIRONMENT, REGION
        union
        select 'CONTRACTS_CLIENT_USER_TYPE_COUNTS' as load_object
              , MAX(REPORT_DATE) as REPORT_DATE
              , ENVIRONMENT
              , REGION
        FROM APTTUS_DW.SF_PRODUCTION.CONTRACTS_CLIENT_USER_TYPE_COUNTS
        GROUP BY ENVIRONMENT, REGION
        union
        select 
        
        SELECT count(*) , to_date(REQUEST_TIMESTAMP), REQUEST_REGION
        FROM SIGN_SIGNINGREQUEST_EVENT
)        
--, add_filter
        select load_object
              , REPORT_DATE
              , ENVIRONMENT
              , REGION
              , CASE 
                  when load_object like 'CONTRACT%'
                   and ENVIRONMENT = 'sandbox'
                   and REGION = 'EU'
                    then 1
                  when load_object like 'CONTRACT%'
                   and ENVIRONMENT = 'datacenter'
                   and REGION = 'NA'
                    then 1
                 else 0
                END AS OLD_FILTER       
        from union_it       
        
        order by 2 asc
;

