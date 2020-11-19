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
;
        select  PACKAGE_ID
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

;
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

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
COMMENT = 'build out app analytics summary joined for C1 and A1 Summarized at Org and Package
'
AS 
WITH unionit AS (
        SELECT    'Apttus1.0' AS CRM 
                , "MONTH"
                , ORGANIZATION_ID
                , PACKAGE_ID
                , UPPER(MANAGED_PACKAGE_NAMESPACE) AS MANAGED_PACKAGE_NAMESPACE
                , CUSTOM_ENTITY
                , CUSTOM_ENTITY_TYPE
                , USER_ID_TOKEN
                , USER_TYPE
                , NUM_CREATES
                , NUM_READS
                , NUM_UPDATES
                , NUM_DELETES
                , NUM_VIEWS
        FROM
                APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY
        union
        SELECT    'Conga1.0' AS CRM
                , "MONTH"
                , ORGANIZATION_ID
                , PACKAGE_ID
                , UPPER(MANAGED_PACKAGE_NAMESPACE) AS MANAGED_PACKAGE_NAMESPACE
                , CUSTOM_ENTITY
                , CUSTOM_ENTITY_TYPE
                , USER_ID_TOKEN
                , USER_TYPE
                , NUM_CREATES
                , NUM_READS
                , NUM_UPDATES
                , NUM_DELETES
                , NUM_VIEWS
        FROM
                APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY_C1
)
, totalit as (
        SELECT    CRM
                , TO_NUMBER(SUBSTRING("MONTH", 1,4)) AS REPORT_YEAR
                , TO_NUMBER(SUBSTRING("MONTH", 6,2)) AS REPORT_MONTH
                , "MONTH" as REPORT_YEAR_MONTH
                , ORGANIZATION_ID
                , PACKAGE_ID
                , MANAGED_PACKAGE_NAMESPACE
                , COALESCE(COUNT(DISTINCT USER_ID_TOKEN), 0) AS MONTHLY_ACTIVE_USERS
                , COALESCE(SUM(NUM_CREATES), 0) AS NUM_CREATES
                , COALESCE(SUM(NUM_READS), 0) AS NUM_READS
       	        , COALESCE(SUM(NUM_UPDATES), 0) AS NUM_UPDATES
                , COALESCE(SUM(NUM_DELETES), 0) AS NUM_DELETES
                , COALESCE(SUM(NUM_VIEWS), 0) AS NUM_VIEWS  
       from unionit              
       GROUP BY  CRM
                , "MONTH"
                , ORGANIZATION_ID
                , PACKAGE_ID
                , MANAGED_PACKAGE_NAMESPACE          
)
        SELECT    CRM
                , D."Date" as REPORT_DATE
                , A.REPORT_YEAR_MONTH
                , ORGANIZATION_ID
                , PACKAGE_ID
                , MANAGED_PACKAGE_NAMESPACE
                , MONTHLY_ACTIVE_USERS 
                , NUM_CREATES
                , NUM_READS
                , NUM_UPDATES
                , NUM_DELETES
                , NUM_VIEWS
                , (NUM_READS + NUM_ViEWS) AS ACCESS_ACTIVITY
                , (NUM_CREATES + NUM_UPDATES + NUM_DELETES) AS MANIPULATION_ACTIVITY
                , (NUM_READS + NUM_ViEWS + NUM_CREATES + NUM_UPDATES + NUM_DELETES) AS MONTHLY_ACTIVITY
        FROM                                  totalit A
        LEFT OUTER JOIN                       APTTUS_DW.SF_PRODUCTION."Dates" D
                            ON  A.REPORT_YEAR = D."Calendar_Year"
                            AND A.REPORT_MONTH = D."Calendar_Month"
                            AND D."Day" = 1
;
	

	