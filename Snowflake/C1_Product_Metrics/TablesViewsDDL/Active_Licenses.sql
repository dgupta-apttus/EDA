--DROP VIEW APTTUS_DW.PRODUCT."Active_Licenses";

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."Active_Licenses"
COMMENT = 'Compute fields and windows for further License / Productline processing.  Current view'
AS 
with recent_MAU_date as ( 
        SELECT MAX(ACTIVITY_MONTH_DATE) as CURRENT_ACTIVITY_MONTH
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
)
, current_MAU_C1  as (
        select 
              'Conga1.0' as CRM_SOURCE
             , ORG_SOURCE
             , SOURCE_ORG_ID
             , PRODUCT_LINE as PRODUCT
             , PACKAGE_NAMESPACE
             , UNIQUE_USERS AS MONTHLY_UNIQUE_USERS
             , PERCENT_SERVICE_EVENTS
        FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY             
        where ACTIVITY_MONTH_DATE  = (SELECT CURRENT_ACTIVITY_MONTH from recent_MAU_date)
)
, current_MAU_A1  as (
        select 
              'Apttus1.0' as CRM_SOURCE
             , "Subscriber Org ID" AS CUSTOMER_ORG
             , "Package ID" as PACKAGE_ID
             , count(distinct "User ID") AS MONTHLY_UNIQUE_USERS             
        FROM APTTUS_DW.SF_PRODUCTION.PRODUCT_APPANALYTICS_SUMMARY
        WHERE "DATE" = (SELECT CURRENT_ACTIVITY_MONTH from recent_MAU_date)
        GROUP BY "Subscriber Org ID"
                , "Package ID" 
)
select A.CRM_SOURCE AS "CRM"
     , A.ACCOUNT_ID AS "Account ID"
     , COALESCE(C.ACCOUNT_NAME, A.ACCOUNT_NAME) AS "Account Name" 
     , A.CUSTOMER_ORG AS "Customer Org"
     , A.ORG_PACKAGE AS "CK1" 
     , A.PRODUCTFAMILY AS "Product Family"
     , A.PRODUCT AS "Product"
     , A.PACKAGE_NAME AS "Package Name"
     , A.PACKAGE_ID AS "Package ID"
     , A.PACKAGE_VERSION_ID AS "Package Version ID"
     , A.LICENSE_SEAT_TYPE AS "License Seat Type"
     , A.SEATS AS "Licensed Seats" 
     , A.USED_LICENSES AS "Used Seats"
     , Case
         WHEN A.CRM_SOURCE = 'Conga1.0'
           THEN COALESCE(B.MONTHLY_UNIQUE_USERS, 0) 
         WHEN A.CRM_SOURCE = 'Apttus1.0'
           THEN COALESCE(D.MONTHLY_UNIQUE_USERS, 0)   
        ELSE NULL
       END AS "Monthly Unique Users"   
     , A.STATUS AS "License Status"
     , A.ORG_STATUS AS "Org Status"
     , A.IS_SANDBOX AS "Is Sandbox"
     , A.LICENSE_NAME AS "License Name" 
     , A.LICENSE_ID AS "License ID"
     , CASE
         WHEN A.LICENSE_SEAT_TYPE = 'Site'
           THEN NULL
         WHEN A.LICENSE_SEAT_TYPE = 'Seats'
          AND A.SEATS > 0
           THEN ((A.USED_LICENSES/A.SEATS)*100)::INTEGER
        ELSE 0
       END AS "Assigned Ratio"
     , CASE
         WHEN  A.USED_LICENSES > 0
          AND A.CRM_SOURCE = 'Conga1.0'
           THEN ((COALESCE(B.MONTHLY_UNIQUE_USERS, 0)/A.USED_LICENSES)*100)::INTEGER
         WHEN A.USED_LICENSES > 0
          AND A.CRM_SOURCE = 'Apttus1.0'
           THEN ((COALESCE(D.MONTHLY_UNIQUE_USERS, 0)/A.USED_LICENSES)*100)::INTEGER
         WHEN A.LICENSE_SEAT_TYPE = 'Site'
           THEN NULL
        ELSE 0
       END AS "Usage Ratio"
     , CASE
         WHEN A.LICENSE_SEAT_TYPE = 'Site'
           THEN NULL
         WHEN  A.SEATS > 0
          AND A.CRM_SOURCE = 'Conga1.0'
           THEN ((COALESCE(B.MONTHLY_UNIQUE_USERS, 0)/A.SEATS)*100)::INTEGER
         WHEN A.SEATS > 0
          AND A.CRM_SOURCE = 'Apttus1.0'
           THEN ((COALESCE(D.MONTHLY_UNIQUE_USERS, 0)/A.SEATS)*100)::INTEGER
        ELSE 0
       END AS "Usage/Purchased Ratio"       
     , B.PERCENT_SERVICE_EVENTS::INTEGER AS "Service Events Percentage"            
     , TO_DATE(A.INSTALL_DATE) AS "Install Date"
     , A.UNINSTALL_DATE AS "Uninstall Date" 
     , A.EXPIRATION_DATE AS "Expiration Date"
     , A.LAST_ACTIVITY_DATE AS "Last Activity"
     , A.EXPIRATION_DATE_STRING AS "Expiration Text"
     , A.INSTALL_DATE_STRING AS "Install Text"
     , A.CUSTOM_LAST_MOD as "Custom Last Modified Date"
     , C.ACCOUNT_NAME AS "Acc Account Name"      
     , A.ACCOUNT_NAME AS "LMA Account Name"
     , A.CUSTOMER_ORG_18
     , A.CUSTOMER_ORG_15 
     , A.OLDCRM_ACCOUNT_ID    
FROM                      APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT A
LEFT OUTER JOIN           current_MAU_C1 B
                  ON  UPPER(A.CRM_SOURCE) = UPPER(B.CRM_SOURCE)
                  AND A.CUSTOMER_ORG = B.SOURCE_ORG_ID
                  AND A.PRODUCT = B.PRODUCT
LEFT OUTER JOIN           APTTUS_DW.SF_PRODUCTION."Account_C2" C
                  ON  UPPER(A.CRM_SOURCE) = UPPER(C.SOURCE)
                  AND A.ACCOUNT_ID = C.ACCOUNTID_18__C 
LEFT OUTER JOIN           current_MAU_A1 D
                  ON  UPPER(A.CRM_SOURCE) = UPPER(D.CRM_SOURCE)
                  AND A.CUSTOMER_ORG = D.CUSTOMER_ORG
                  AND A.PACKAGE_ID_AA = D.PACKAGE_ID  -- PACKAGE_ID_AA is the one used for App Analytics                                                   
WHERE A.STATUS = 'Active'
  and A.ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
  and A.IS_SANDBOX = false   
  and A.EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED','EXPIRED')
  and A.ACCOUNT_ID is not null 
  and A.SELECT1_FOR_PACKAGE_ID = 1 -- this removes Duplicates produced when an org has more than 1 license ID per package_id (rare c1 situation)       
;  
