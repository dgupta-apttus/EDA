select count(*)
      , month
      , PRODUCT
      , CASE
           WHEN C2_ACCOUNT = 'Not Found'
              THEN 'Not Found'
         ELSE 'Found'     
        END as C2_ACCOUNT_FOUND
      , STATUS
      , ORG_STATUS        
from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2
group by month, PRODUCT
    , C2_ACCOUNT_FOUND, STATUS
    , ORG_STATUS 
;

select distinct custom_entity_type
from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2
;

select USER_TYPE, count(*)
from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2
group by USER_TYPE
;

select count(*), custom_entity, custom_entity_type, package_name
from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2
where CRM_SOURCE IN ('Conga1.0')
group by custom_entity, custom_entity_type, package_name
;

with userAndType as (
        select distinct 
        	USER_ID_TOKEN
	      , USER_TYPE
        from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2        
)
        select count(*), USER_ID_TOKEN
        from userAndType
        group by USER_ID_TOKEN
        having count(*) > 1        
;

select * from APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2 
where USER_ID_TOKEN in ('005-niYmmJFJI/n6a3SWoAWJRvTDQjzvzqKj36oibbYHUaU=')
;

with pairs as (
        select distinct 
        	CUSTOM_ENTITY
	      , CUSTOM_ENTITY_TYPE
        from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2        
)
        select count(*), CUSTOM_ENTITY
        from pairs
        group by CUSTOM_ENTITY
        having count(*) > 1        
;


with get_list as (
        select  CUSTOM_ENTITY
	      , CUSTOM_ENTITY_TYPE
	      , listagg(DISTINCT PACKAGE_NAME, ',') within group (ORDER BY PACKAGE_NAME) AS PACKAGE_LIST
	      , count(DISTINCT PACKAGE_NAME) as PACKAGE_COUNT
	      , listagg(DISTINCT MANAGED_PACKAGE_NAMESPACE, ',') within group (ORDER BY MANAGED_PACKAGE_NAMESPACE) AS NAMESPACE_LIST
	      , count(DISTINCT MANAGED_PACKAGE_NAMESPACE) as NAMESPACE_COUNT
	      , listagg(DISTINCT PRODUCT, ',') within group (ORDER BY PRODUCT) AS PRODUCT_LIST
	      , COUNT(DISTINCT PRODUCT) as PRODUCT_COUNT
        from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2 
        group by CUSTOM_ENTITY
	      , CUSTOM_ENTITY_TYPE 
	order by 8 desc      
)
, current_counts as (
        select  CUSTOM_ENTITY
	      , CUSTOM_ENTITY_TYPE
	      , "MONTH" as REPORT_MONTH
              , SUM(NUM_CREATES) AS NUM_CREATES
              , SUM(NUM_READS) AS NUM_READS
       	      , SUM(NUM_UPDATES) AS NUM_UPDATES
              , SUM(NUM_DELETES) AS NUM_DELETES
              , SUM(NUM_VIEWS) AS NUM_VIEWS
              , COUNT(DISTINCT USER_ID_TOKEN) AS UNIQUE_USERS
              , COUNT(DISTINCT CUSTOMER_ORG_15) AS UNIQUE_ORGS
              , COUNT(DISTINCT C2_ACCOUNT) AS UNIQUE_ACCOUNTS
        from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2 
        where "MONTH" = (SELECT MAX("MONTH") from APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2)
        group by CUSTOM_ENTITY
	      , CUSTOM_ENTITY_TYPE
	      , "MONTH"
)
        select
      	        A.PRODUCT_LIST
              , A.CUSTOM_ENTITY
	      , A.CUSTOM_ENTITY_TYPE
	      , A.PACKAGE_LIST
	      , A.PACKAGE_COUNT
	      , A.NAMESPACE_LIST
	      , A.NAMESPACE_COUNT
	      , A.PRODUCT_COUNT
	      , B.REPORT_MONTH
              , B.NUM_CREATES
              , B.NUM_READS
       	      , B.NUM_UPDATES
              , B.NUM_DELETES
              , B.NUM_VIEWS
              , B.UNIQUE_USERS
              , B.UNIQUE_ORGS
              , B.UNIQUE_ACCOUNTS
        from                        get_list A
        left outer join             current_counts B
                          ON  A.CUSTOM_ENTITY = B.CUSTOM_ENTITY
	                  AND A.CUSTOM_ENTITY_TYPE = B.CUSTOM_ENTITY_TYPE
	ORDER BY A.PRODUCT_COUNT desc
	       , A.PRODUCT_LIST asc                  	      
;

with pairs as (
        select distinct 
        	PACKAGE_ID
	      , UPPER(MANAGED_PACKAGE_NAMESPACE) AS MANAGED_PACKAGE_NAMESPACE
        from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2        
)
        select count(*), MANAGED_PACKAGE_NAMESPACE
        from pairs
        group by MANAGED_PACKAGE_NAMESPACE
        having count(*) > 1     
;
        select count(*), PACKAGE_ID
        from pairs
        group by PACKAGE_ID
        having count(*) > 1        
;
select *
from  APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2  
where PACKAGE_ID in ('a0150000014P5LQAA0')  
;
  
WITH views_reduction as (  
        select 	"MONTH"
                , ORGANIZATION_ID
                , PACKAGE_ID
                , MANAGED_PACKAGE_NAMESPACE
                , REPLACE(CUSTOM_ENTITY, HEX_DECODE_STRING(27), '') AS CUSTOM_ENTITY
        --	, CUSTOM_ENTITY_TYPE
                , USER_ID_TOKEN
                , USER_TYPE
        --	, NUM_CREATES
        --	, NUM_READS
        --	, NUM_UPDATES
        --	, NUM_DELETES
                , NUM_VIEWS
        FROM                        APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY_C1
        WHERE NUM_VIEWS > 0
)
, make_list as (
        SELECT listagg(DISTINCT CUSTOM_ENTITY, ',') within group (ORDER BY CUSTOM_ENTITY) AS CUSTOM_ENTITY_LIST
        FROM views_reduction
)


	SELECT *
	FROM views_reduction
	pivot(sum(NUM_VIEWS) for CUSTOM_ENTITY in ('(select CUSTOM_ENTITY_LIST FROM make_list)'))
;


