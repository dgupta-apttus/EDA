CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."License_Monthly_History"
COMMENT = 'Month by month veiw of license and user counts
-- 2020/12/16 switching old> Master_Package_List for NEW> MASTER_PRODUCT_PACKAGE_MAPPING as Package_id lookup -- gdw
-- 2020/12/22 adding CRM attribute to License Name and adding the composite License status -- gdw 
-- 2021/01/28 switch to standard PRODUCT.APPANALYTICS_SUMMARY from SF_PRODUCTION.PRODUCT_APPANALYTICS_SUMMARY -- gdw
'
AS   
WITH A1_MAU as (
        select 
               CRM_SOURCE
             , "Subscriber Org ID" AS CUSTOMER_ORG
             , "Package ID" as LMA_PACKAGE_ID
             , "DATE" as ACTIVITY_MONTH_DATE
             , count(distinct "User ID") AS MONTHLY_UNIQUE_USERS   
             , (select MAX(PACKAGEID) FROM APTTUS_DW.PRODUCT."Master_Package_List" WHERE LMA_PACKAGE_ID = A."Package ID"
               ) AS PACKAGE_ID -- PACKAGE_ID_AA is the one used for App Analytics          
        FROM APTTUS_DW.PRODUCT.APPANALYTICS_SUMMARY A
        WHERE CRM_SOURCE = 'Apttus1.0'
        GROUP BY "Subscriber Org ID"
                , "Package ID" 
                , "DATE"
                , CRM_SOURCE
)
        SELECT
               A.ACCOUNT_ID AS "Account ID"
             , COALESCE(C.ACCOUNT_NAME, A.ACCOUNT_NAME) AS "Account Name"
             , A.ACCOUNT_NAME AS "Account Name on LMA"
             , CASE
                 WHEN A.ACTIVE_SEAT_TYPE = 'Site'
                   THEN NULL
                 WHEN A.ACTIVE_SEAT_TYPE = 'Seats'
                  AND A.ACTIVE_SEATS > 0
                   THEN ((A.ACTIVE_USED/A.ACTIVE_SEATS)*100)::INTEGER
                ELSE 0
               END AS "Assigned Ratio" 
             , A.CUSTOMER_ORG_15 || '-' || A.PACKAGE_ID AS CK1_ORG_PACKAGE
             ,  A.CRM_SOURCE AS "CRM"
             , A.CUSTOMER_ORG AS "Customer Org"
             , A.EXPIRATION_DATE AS "Expiration Date"
             , A.EXPIRATION_DATE_STRING AS "Expiration Text"
             , TO_DATE(A.INSTALL_DATE) AS "Install Date"
             , A.INSTALL_DATE_STRING AS "Install Text"
             , A.LAST_ACTIVITY_DATE AS "Last Activity"             
             , A.PRIMARY_LICENSE_ID AS "License ID"   
             , A.LICENSE_NAME || '-' || SUBSTR(A.CRM_SOURCE, 1, 1)  AS "License Name"   
             , A.ACTIVE_SEAT_TYPE AS "License Seat Type"        
                  
             , A.PACKAGE_ID AS "Package ID"
             , A.PACKAGE_NAME AS "Package Name"
             , A.PACKAGE_VERSION_ID AS "Package Version ID"
                                                    
             , A.PRODUCT AS "Product"             
             , A.PRODUCTFAMILY AS "Product Family"
             , A.REPORTING_DATE AS "Report Month Date" 

             , A.ACTIVE_SEATS AS "Seats Active"
             , A.NONPROD_SEATS AS "Seats Non-Prod"
             , A.SANDBOX_SEATS AS "Seats Sandbox"

             , COALESCE(B.PERCENT_SERVICE_EVENTS, 0)::INTEGER AS "Service Events Percentage"
             , CASE 
                  WHEN A.IS_SANDBOX = true
                    THEN 'Sandbox'
                  WHEN A.IS_SANDBOX = false
                   AND (UPPER(A.STATUS) <> 'ACTIVE'
                        OR A.ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
                       )
                    THEN 'Not Production'
                  WHEN UPPER(A.STATUS) = 'ACTIVE'
                   AND A.ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
                   AND A.IS_SANDBOX = false
                   AND A.ACCOUNT_ID is null
                    THEN 'Active w/o Acc'
                  WHEN UPPER(A.STATUS) = 'ACTIVE'
                   AND A.ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
                   AND A.IS_SANDBOX = false
                    THEN 'Active'                     
                 ELSE 'Unknown'
               END                              AS "License Status"  
             , A.IS_SANDBOX AS "Status - Sandbox"
             , A.STATUS AS "Status - License"
             , A.ORG_STATUS AS "Status - Org"   
             , A.UNINSTALL_DATE AS "Uninstall Date"              
             , Case
                 WHEN A.CRM_SOURCE = 'Conga1.0'
                   THEN COALESCE(B.UNIQUE_USERS, 0) 
                 WHEN A.CRM_SOURCE = 'Apttus1.0'
                   THEN COALESCE(D.MONTHLY_UNIQUE_USERS, 0)   
                ELSE NULL
               END AS "Unique Users"   
             , CASE
                 WHEN A.ACTIVE_SEAT_TYPE = 'Seats'  
                  AND A.ACTIVE_USED > 0
                  AND A.CRM_SOURCE = 'Conga1.0'
                   THEN ((COALESCE(B.UNIQUE_USERS, 0)/A.ACTIVE_USED)*100)::INTEGER
                 WHEN A.ACTIVE_SEAT_TYPE = 'Seats'
                  AND A.ACTIVE_USED > 0
                  AND A.CRM_SOURCE = 'Apttus1.0'
                   THEN ((COALESCE(D.MONTHLY_UNIQUE_USERS, 0)/A.ACTIVE_USED)*100)::INTEGER
                ELSE NULL
               END AS "Usage Ratio" 
             , CASE
                 WHEN A.ACTIVE_SEAT_TYPE <> 'Seats'
                   THEN NULL
                 WHEN  A.ACTIVE_SEATS > 0
                  AND A.CRM_SOURCE = 'Conga1.0'
                   THEN ((COALESCE(B.UNIQUE_USERS, 0)/A.ACTIVE_SEATS)*100)::INTEGER
                 WHEN A.ACTIVE_SEATS > 0
                  AND A.CRM_SOURCE = 'Apttus1.0'
                   THEN ((COALESCE(D.MONTHLY_UNIQUE_USERS, 0)/A.ACTIVE_SEATS)*100)::INTEGER
                ELSE 0
               END AS "Usage/Purchased Ratio" 
             , A.ACTIVE_USED AS "Used Active Seats"
             , A.NONPROD_USED AS "Used Non-Product Seats"
             , A.SANDBOX_USED AS "Used Sandbox Seats"
             , A.CUSTOMER_ORG_18 AS "X18 Customer Org"
             , A.CUSTOMER_ORG_15 AS "X15 Customer Org"             
--, C.ACCOUNT_NAME AS "Acc Account Name"      
-- , A.CUSTOM_LAST_MOD as "Custom Last Modified Date"
-- , A.OLDCRM_ACCOUNT_ID                
        FROM                      APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY A
        LEFT OUTER JOIN           APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY B
                          ON  UPPER(A.CRM_SOURCE) = UPPER(B.CRM_SOURCE)
                          AND A.CUSTOMER_ORG = B.SOURCE_ORG_ID
                          AND A.PACKAGE_ID = B.PACKAGE_ID
                          AND A.REPORTING_DATE = B.ACTIVITY_MONTH_DATE                    
        LEFT OUTER JOIN           A1_MAU D
                          ON  UPPER(A.CRM_SOURCE) = UPPER(D.CRM_SOURCE)
                          AND A.CUSTOMER_ORG = D.CUSTOMER_ORG
                          AND A.PACKAGE_ID = D.PACKAGE_ID
                          AND A.REPORTING_DATE = D.ACTIVITY_MONTH_DATE
        LEFT OUTER JOIN           APTTUS_DW.SF_PRODUCTION."Account_C2" C
                          ON  UPPER(A.CRM_SOURCE) = UPPER(C.SOURCE)
                          AND A.ACCOUNT_ID = C.ACCOUNTID_18__C;
                          


