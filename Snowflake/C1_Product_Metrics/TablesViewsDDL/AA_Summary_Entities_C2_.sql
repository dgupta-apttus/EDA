CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."AA_Summary_Entities_C2"
COMMENT = 'just a list of the current entities in App Analytics for C2'
AS 
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
              , COUNT(DISTINCT CUSTOMER_ORG) AS UNIQUE_ORGS
              , COUNT(DISTINCT LMA_ACCOUNT_ID) AS UNIQUE_ACCOUNTS   
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




