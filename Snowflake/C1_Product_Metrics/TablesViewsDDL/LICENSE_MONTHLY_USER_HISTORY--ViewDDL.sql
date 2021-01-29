CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.LICENSE_MONTHLY_USER_HISTORY
COMMENT = 'Month by month veiw of license and user tokens
-- 2021/01/12 adapted from License_Monthly_History - gdw
'
AS 
WITH remove_AA_entity_level as (
        select 
               CRM_SOURCE
             , "Subscriber Org ID" AS CUSTOMER_ORG
             , "Package ID" AS PACKAGE_ID -- PACKAGE_ID_AA is the one used for App Analytics  
             , "LMA Package ID" as LMA_PACKAGE_ID
             , "DATE" as ACTIVITY_MONTH_DATE
             , "User Type" AS USER_TYPE 
             , "User ID" AS USER_ID  
             , SUM("Creates") as CREATES
             , SUM("Deletes") as DELETES
             , SUM("Reads") as REC_READS
             , SUM("Updates") as UPDATES
             , SUM("Views") as VIEWS      
        FROM APTTUS_DW.PRODUCT.APPANALYTICS_SUMMARY A
        GROUP BY CRM_SOURCE
                , "Subscriber Org ID"
                , "Package ID" 
                , "LMA Package ID"
                , "DATE"
                , "User Type" 
                , "User ID"  
)
-- join to License Package
        SELECT A.CRM_SOURCE
             , A.CUSTOMER_ORG
             , A.PACKAGE_ID -- PACKAGE_ID_AA is the one used for App Analytics 
             , A.LMA_PACKAGE_ID
             , A.ACTIVITY_MONTH_DATE
             , A.USER_TYPE
             , A.USER_ID   
             , COALESCE(CREATES, 0) as CREATES
             , COALESCE(DELETES, 0) as DELETES
             , COALESCE(REC_READS, 0) as REC_READS
             , COALESCE(UPDATES, 0) as UPDATES
             , COALESCE(VIEWS, 0) as VIEWS
             , COALESCE(B.ACCOUNT_ID, 'NOT FOUND') AS ACCOUNT_ID
             , COALESCE(B.PRIMARY_LICENSE_ID, 'NOT FOUND') AS LICENSE_ID   
             , COALESCE(B.LICENSE_NAME || '-' || SUBSTR(B.CRM_SOURCE, 1, 1), 'NOT FOUND') AS LICENSE_NAME 
             , B.CUSTOMER_ORG_18 
             , B.CUSTOMER_ORG_15  
        FROM                     remove_AA_entity_level A
        LEFT OUTER JOIN          APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY B
                          ON  UPPER(A.CRM_SOURCE) = UPPER(B.CRM_SOURCE)
                          AND A.CUSTOMER_ORG = B.CUSTOMER_ORG_15
                          AND A.PACKAGE_ID = B.PACKAGE_ID
                          AND A.ACTIVITY_MONTH_DATE = B.REPORTING_DATE
        WHERE A.ACTIVITY_MONTH_DATE >= '2020-10-01'                   
;
