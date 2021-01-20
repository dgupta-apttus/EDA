
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.APP_ANALYTICS_ENTITY_FL
COMMENT = '
Build out app analytics summary joined for C1 and A1 Summarized at Org, Package, User, and Entity
Combine Activity Measures with License for data from App Analytics
'
AS  
with remove_nulls as (
        SELECT CRM_SOURCE AS CRM
             , "DATE" AS "Report Date"
             , "Subscriber Org ID"
             , "Package ID"
             , "LMA Package ID"
             , "Namespace"
             , "User Type"
             , "User ID"
             , "Entity"
             , "Entity Type"
             , COALESCE("Creates", 0) AS "Creates"
             , COALESCE("Deletes", 0) AS "Deletes"
             , COALESCE("Reads", 0) AS "Reads"
             , COALESCE("Updates", 0) AS "Updates"
             , COALESCE("Views", 0) AS "Views"
             , "CK"
             , "Entity_CK" 
        FROM APTTUS_DW.PRODUCT.APPANALYTICS_SUMMARY             
)
        SELECT CRM
             , "Report Date"
             , "Subscriber Org ID"
             , "Package ID"
             , "LMA Package ID"
             , "Namespace"
             , "User Type"
             , "User ID"
             , "Entity"
             , "Entity Type"
             , "Creates"
             , "Deletes"
             , "Reads"
             , "Updates"
             , "Views"
             , "CK"
             , "Entity_CK" 
        --
             , ("Reads" + "Views") AS "Access Activity"
             , ("Creates" + "Updates" + "Deletes") AS "Manipulation Activity"
             , ("Reads" + "Views" + "Creates" + "Updates" + "Deletes") AS "Monthly Activity"     
             , ("Creates" + "Updates" + "Deletes" +  "Views") AS "CUDV Activity"
-- license info
                , COALESCE(C.ACTIVE_LICENSE_COUNT, 0) AS "Active License Count"
                , COALESCE(C.ACCOUNT_ID, 'Not Found') AS "Account ID"
                , COALESCE(C.ACCOUNT_NAME, 'Not Found') AS "Account Name on LMA"                
                , COALESCE(C.PRIMARY_LICENSE_ID, 'Not Found') AS "License ID"   
                , COALESCE(C.LICENSE_NAME, 'Not Found') AS "License Name"  
                , C.ACTIVE_SEAT_TYPE AS "License Seat Type"
                , C.IS_SANDBOX AS "Status - Sandbox"
                , C.STATUS AS "Status - License"
                , C.ORG_STATUS AS "Status - Org"                               
                , COALESCE(C.ACTIVE_SEATS, 0) AS "Seats Active"
                , COALESCE(C.ACTIVE_USED, 0) AS "Used Active Seats"   
                , COALESCE(C.NONPROD_SEATS, 0) AS "Seats Non-Prod"
                , COALESCE(C.SANDBOX_SEATS, 0) AS "Seats Sandbox"                  
                , C.EXPIRATION_DATE AS "Expiration Date"
                , C.EXPIRATION_DATE_STRING AS "Expiration Text"
                , TO_DATE(C.INSTALL_DATE) AS "Install Date"
                , C.INSTALL_DATE_STRING AS "Install Text"                
                , C.UNINSTALL_DATE AS "Uninstall Date"  
        FROM remove_nulls A
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY C
                         ON   A."Subscriber Org ID" = C.CUSTOMER_ORG_15 
                         AND  A."Package ID" = C.PACKAGE_ID
                         AND  A."Report Date" = C.REPORTING_DATE         
;


/* OLDER VERSION replaced on 2021/01/19
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.APP_ANALYTICS_ENTITY_FL
COMMENT = 'Combine Activity Scores with License for data from App Analytics'
AS  
        SELECT    A.CRM
                , 'AppAnalytics'                           AS "Data Source"  
                , 'SALESFORCE'                             AS "Org Source"  
                , A.ORGANIZATION_ID                        AS "Customer Organization" 
                , A.REPORT_DATE                            AS "Report Date"
                , Coalesce(B.LMA_PACKAGE_ID, A.PACKAGE_ID) AS "LMA Package ID"
                , B.PACKAGE_ID                             AS "Package ID"                 
                , A.MANAGED_PACKAGE_NAMESPACE              AS "Package Namespace"
                , A.CUSTOM_ENTITY                          AS "Entity"
                , A.CUSTOM_ENTITY_TYPE                     AS "Entity Type"                   
                , A.MONTHLY_ACTIVE_USERS                   AS "Monthly Active Users" 
                , A.NUM_CREATES                            AS "Creates"
                , A.NUM_READS                              AS "Reads"
                , A.NUM_UPDATES                            AS "Updates"
                , A.NUM_DELETES                            AS "Deletes" 
                , A.NUM_VIEWS                              AS "Views"
                , A.ACCESS_ACTIVITY                        AS "Access"
                , A.MANIPULATION_ACTIVITY                  AS "Manipulation"
                , A.MONTHLY_ACTIVITY                       AS "Total Activity"
-- license info
                , COALESCE(C.ACTIVE_LICENSE_COUNT, 0) AS "Active License Count"
                , COALESCE(C.ACCOUNT_ID, 'Not Found') AS "Account ID"
                , COALESCE(C.ACCOUNT_NAME, 'Not Found') AS "Account Name on LMA"                
                , COALESCE(C.PRIMARY_LICENSE_ID, 'Not Found') AS "License ID"   
                , COALESCE(C.LICENSE_NAME, 'Not Found') AS "License Name"  
                , C.ACTIVE_SEAT_TYPE AS "License Seat Type"
                , C.IS_SANDBOX AS "Status - Sandbox"
                , C.STATUS AS "Status - License"
                , C.ORG_STATUS AS "Status - Org"                               
                , COALESCE(C.ACTIVE_SEATS, 0) AS "Seats Active"
                , COALESCE(C.ACTIVE_USED, 0) AS "Used Active Seats"   
                , COALESCE(C.NONPROD_SEATS, 0) AS "Seats Non-Prod"
                , COALESCE(C.SANDBOX_SEATS, 0) AS "Seats Sandbox"                  
                , C.EXPIRATION_DATE AS "Expiration Date"
                , C.EXPIRATION_DATE_STRING AS "Expiration Text"
                , TO_DATE(C.INSTALL_DATE) AS "Install Date"
                , C.INSTALL_DATE_STRING AS "Install Text"                
                , C.UNINSTALL_DATE AS "Uninstall Date"                                
        FROM                  	APTTUS_DW.PRODUCT.APP_ANALYTICS_ENTITY_SUMMARY A 
        LEFT OUTER JOIN         APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING B
                         ON  A.PACKAGE_ID = B.LMA_PACKAGE_ID
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY C
                         ON   A.ORGANIZATION_ID = C.CUSTOMER_ORG_15 
                         AND  B.PACKAGE_ID = C.PACKAGE_ID
                         AND  A.REPORT_DATE = C.REPORTING_DATE                                
;

-- testing --
select count(*) from APTTUS_DW.PRODUCT.APP_ANALYTICS_ENTITY_FL;
select * from APTTUS_DW.PRODUCT.APP_ANALYTICS_ENTITY_FL;

select "Package ID", count(*) 
from APTTUS_DW.PRODUCT.APP_ANALYTICS_ENTITY_FL
group by "Package ID"
order by 1
;
