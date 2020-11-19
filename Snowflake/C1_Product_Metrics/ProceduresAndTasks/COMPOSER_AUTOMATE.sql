--INSERT INTO automate_percent_one (
--environment_id, score_date_id, product_line, automate_percent_1
--)

with total_merges as (     
        select SALESFORCE_ORG_ID 
             , Year(MERGE_TIMESTAMP) AS ACTIVITY_YEAR
             , Month(MERGE_TIMESTAMP) AS ACTIVITY_MONTH    
             , count(*) as total_merges           
        from APTTUS_DW.SF_PRODUCTION.COMPOSER_MERGE_EVENT_LOAD   
--        where Year(MERGE_TIMESTAMP) = YEAR(dateadd(month, -1, CURRENT_DATE()))
--          and Month(MERGE_TIMESTAMP) = MONTH(dateadd(month, -1, CURRENT_DATE()))                 
        group by SALESFORCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH
)
, service_event_merges as ( 
        select SALESFORCE_ORG_ID 
             , Year(MERGE_TIMESTAMP) AS ACTIVITY_YEAR
             , Month(MERGE_TIMESTAMP) AS ACTIVITY_MONTH    
             , count(*) as service_event_merges
        from APTTUS_DW.SF_PRODUCTION.COMPOSER_MERGE_EVENT_LOAD     
        where  event_type in ('Conductor','Workflow')                     
--          and Year(MERGE_TIMESTAMP) = YEAR(dateadd(month, -1, CURRENT_DATE()))
--          and Month(MERGE_TIMESTAMP) = MONTH(dateadd(month, -1, CURRENT_DATE()))                 
        group by SALESFORCE_ORG_ID, ACTIVITY_YEAR, ACTIVITY_MONTH
)
--, percent_service_events as (
        select 'SALESFORCE' AS ORG_SOURCE
             , A.SALESFORCE_ORG_ID
             , A.total_merges
             , B.service_event_merges
             , case when A.total_merges > 0
                   then (B.service_event_merges*100/A.total_merges)
                else -1
               end as percent_service_events 
             , A.ACTIVITY_YEAR
             , A.ACTIVITY_MONTH  
             , D."Date" as ACTIVITY_MONTH_DATE 
             , 'Conga Composer' as PRODUCT_LINE
        FROM                    service_event_merges B             
        INNER JOIN              total_merges A
                       ON  A.SALESFORCE_ORG_ID = B.SALESFORCE_ORG_ID
                       AND A.ACTIVITY_YEAR = B.ACTIVITY_YEAR
                       AND A.ACTIVITY_MONTH = B.ACTIVITY_MONTH
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" D
                     ON  A.ACTIVITY_YEAR = D."Calendar_Year"
                     AND A.ACTIVITY_MONTH = D."Calendar_Month"
                     AND D."Day" = 1                                     
--)            
;