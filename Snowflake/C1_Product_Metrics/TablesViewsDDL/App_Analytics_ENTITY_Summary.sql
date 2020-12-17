CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.APP_ANALYTICS_ENTITY_SUMMARY
COMMENT = 'build out app analytics summary joined for C1 and A1 Summarized at Org, Package, and Entity
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
                , CUSTOM_ENTITY
                , CUSTOM_ENTITY_TYPE                
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
                , CUSTOM_ENTITY
                , CUSTOM_ENTITY_TYPE
)
        SELECT    CRM
                , D."Date" as REPORT_DATE
                , A.REPORT_YEAR_MONTH
                , ORGANIZATION_ID
                , PACKAGE_ID
                , MANAGED_PACKAGE_NAMESPACE
                , CUSTOM_ENTITY
                , CUSTOM_ENTITY_TYPE
                , MONTHLY_ACTIVE_USERS 
                , NUM_CREATES
                , NUM_READS
                , NUM_UPDATES
                , NUM_DELETES
                , NUM_VIEWS
                , (NUM_READS + NUM_ViEWS) AS ACCESS_ACTIVITY
                , (NUM_CREATES + NUM_UPDATES + NUM_DELETES) AS MANIPULATION_ACTIVITY
                , (NUM_READS + NUM_VIEWS + NUM_CREATES + NUM_UPDATES + NUM_DELETES) AS MONTHLY_ACTIVITY
        FROM                                  totalit A
        LEFT OUTER JOIN                       APTTUS_DW.SF_PRODUCTION."Dates" D
                            ON  A.REPORT_YEAR = D."Calendar_Year"
                            AND A.REPORT_MONTH = D."Calendar_Month"
                            AND D."Day" = 1
;
	

	