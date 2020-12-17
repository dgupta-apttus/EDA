--DROP VIEW APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT;

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
COMMENT = 'Compute fields and windows for further License / Productline processing.  Current view
-- 2020/12/15 switch out old>MASTER_PRODUCT_FAMILY for new>MASTER_PRODUCT_PACKAGE_MAPPING to get product hierarchy fields - GDW
'
AS 
WITH license_with_orgs_override as (
        SELECT
                ACCOUNT_SUSPENDED__C
                , CUSTOMER_ORG_ID__C
                , EXPIRATION_DATE__C
                , A.ID
                , A.ISDELETED
                , A.LASTMODIFIEDBYID
                , A.LASTMODIFIEDDATE
                , A."NAME"
                , A.OWNERID
                , PACKAGE_NAMEFX__C
                , PRODUCTION__C
                , A.RECORDTYPEID
                , SALESFORCE_ORGID__C
                , SFLMA__ACCOUNT__C
                , SFLMA__EXPIRATION_DATE__C
                , SFLMA__EXPIRATION__C
                , SFLMA__INSTALL_DATE__C
                , SFLMA__INSTANCE__C
                , SFLMA__IS_SANDBOX__C
                , SFLMA__LAST_MODIFIED__C
                , SFLMA__LICENSED_SEATS__C
                , SFLMA__LICENSE_STATUS__C
                , SFLMA__LICENSE_TYPE__C
                , SFLMA__ORG_EDITION__C
                , SFLMA__ORG_INSTANCE__C
                , SFLMA__ORG_STATUS__C
                , SFLMA__ORG_TRIAL_EXPIRATION__C
                , SFLMA__ORG_TYPE__C
                , SFLMA__PACKAGE_LICENSE_ID__C
                , SFLMA__PACKAGE_VERSION_NUMBER__C
                , SFLMA__PACKAGE_VERSION__C
                , SFLMA__PACKAGE__C
                , SFLMA__SEATS__C
                , SFLMA__STATUS__C
                , SFLMA__SUBSCRIBER_ORG_ID__C
                , SFLMA__SUBSCRIBER_ORG_IS_SANDBOX__C
                , SFLMA__TRIAL_EXPIRATION_DATE__C
                , SFLMA__USED_LICENSES__C
                , SFLMA__VERSION_NUMBER__C
                , SUSPEND_ACCOUNT__C
                , A.SYSTEMMODSTAMP
                , UNINSTALL_DATE__C
                , A.LASTACTIVITYDATE
                , A.ACTIVITY_DATE
                , CASE
                    WHEN B.CONGA_ENFORCE_LMA__C = false
                     AND A.SFLMA__LICENSE_STATUS__C = 'Active' -- status
                     AND A.SFLMA__ORG_STATUS__C = 'ACTIVE' -- org_status
                     AND B.ORG_TYPE__C = 'Production'
                      THEN 0::BOOLEAN
                   ELSE 1::BOOLEAN
                  END AS CONGA_ENFORCE_LMA__C
                , COALESCE(CONGA_ENFORCE_USER_MANAGEMENT__C, 1::BOOLEAN) as CONGA_ENFORCE_USER_MANAGEMENT__C   
                , B.CONGA_LICENSES__C   
        FROM                               APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT A
        LEFT OUTER JOIN	                   APTTUS_DW.SF_CONGA1_1.SALESFORCE_ORG__C B 
                          ON A.ID = B.COMPOSER_LICENSE__C 
) 
        SELECT  'Conga1.0' AS CRM_SOURCE
                , 'Conga1.0' as DATA_SOURCE  
                , L.ID as LICENSE_ID
                , L.CUSTOMER_ORG_ID__C                          AS CUSTOMER_ORG
                , L.CUSTOMER_ORG_ID__C                          AS CUSTOMER_ORG_18
                , substring(L.SFLMA__SUBSCRIBER_ORG_ID__C,1,15) AS CUSTOMER_ORG_15  
                , P.PRODUCT_FAMILY                          AS PRODUCTFAMILY 
                , P.PRODUCT                                 -- replaces C1 product_line but will still match Assets product_line 
                , P.PACKAGE_NAME                            AS PACKAGE_NAME                         
                , L.PACKAGE_NAMEFX__C                       AS PACKAGE_NAMEFX
                , L.SFLMA__PACKAGE__C                       AS PACKAGE_ID
--                , LEFT(PK.SFLMA__PACKAGE_ID__C,15)          AS PACKAGE_ID_AA -- key to package for app analytics not sure if 15 is right for C1                
                , P.LMA_PACKAGE_ID                          AS PACKAGE_ID_AA
                , L.SFLMA__PACKAGE_VERSION__C               AS PACKAGE_VERSION_ID
                , CUSTOMER_ORG_15 || '-' || PACKAGE_ID      AS ORG_PACKAGE
                , L.SFLMA__LICENSE_STATUS__C                AS STATUS
                , COALESCE(SFLMA__ORG_STATUS__C, 'Unknown') AS ORG_STATUS 
                , L.SFLMA__ACCOUNT__C                       AS ACCOUNT_ID
                , null                                      AS ACCOUNT_NAME  
                , L.SFLMA__IS_SANDBOX__C                    AS IS_SANDBOX
                , CASE
                         WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA COMPOSER'
                           THEN 'APXTCONGA4'
                         WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'SALESFORCE CPQ: CONGA QUOTES'
                           THEN 'APXTCFQ'   
                         WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA INVOICE GENERATION'  
                           THEN 'UNKNOWN COMPOSER'
                         ELSE 'OTHER'  
                  END                                       AS PREDICTED_PACKAGE_NAMESPACE 
                , CASE
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
                      THEN 'Site'
                   ELSE 'Seats'
                  END                                       AS LICENSE_SEAT_TYPE    
                , CASE
                    WHEN L.CONGA_ENFORCE_LMA__C = false
                     and L.CONGA_LICENSES__C is not null
                      THEN L.CONGA_LICENSES__C
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
                      THEN NULL
                    WHEN L.SFLMA__SEATS__C > 0
                     THEN L.SFLMA__SEATS__C
                   ELSE 0               
                  END                                       AS SEATS
                , CASE 
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
                     AND L.SFLMA__USED_LICENSES__C > 1
                      THEN L.SFLMA__USED_LICENSES__C 
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'                     
                      THEN NULL                
                   ELSE coalesce(L.SFLMA__USED_LICENSES__C, 0)
                  END                                       AS USED_LICENSES        
                , L.SFLMA__INSTALL_DATE__C                  AS INSTALL_DATE
                , CASE
                    WHEN UPPER(L.EXPIRATION_DATE__C) <> 'DOES NOT EXPIRE'
                      THEN to_Date(L.EXPIRATION_DATE__C)
                   else NULL   
                  END                                       AS EXPIRATION_DATE
                , CASE
                    WHEN UPPER(L.EXPIRATION_DATE__C) = 'DOES NOT EXPIRE' 
                     AND L.SFLMA__LICENSE_STATUS__C NOT IN ('Uninstalled')
                      THEN UPPER(L.EXPIRATION_DATE__C)
                    WHEN L.SFLMA__LICENSE_STATUS__C IN ('Uninstalled')
                      THEN 'UNINSTALLED'
                    WHEN EXPIRATION_DATE IS NOT NULL
                       AND CURRENT_DATE >= EXPIRATION_DATE
                           THEN 'EXPIRED'
                    WHEN EXPIRATION_DATE IS NOT NULL            
                      THEN 'SET TO EXPIRE'  
                    ELSE 'EXPIRATION UNKNOWN'
                  END                                      AS EXPIRATION_DATE_STRING    
                , L.UNINSTALL_DATE__C                       AS UNINSTALL_DATE
                , COALESCE(DATEDIFF(MONTH, L.SFLMA__INSTALL_DATE__C, COALESCE(L.UNINSTALL_DATE__C, EXPIRATION_DATE, CURRENT_DATE)),0) AS MONTHS_INSTALLED
                , CASE 
                    WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL AND MONTHS_INSTALLED > 12
                      THEN 'INSTALLED FOR ' || DATEDIFF(YEAR,  L.SFLMA__INSTALL_DATE__C, COALESCE(L.UNINSTALL_DATE__C, CURRENT_DATE)) || ' YEARS'
                    WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL
                      THEN 'INSTALLED FOR ' || MONTHS_INSTALLED || ' MONTHS'  
                    ELSE 'INSTALL DATE NOT KNOWN'  
                  END                                       AS INSTALL_DATE_STRING  
                , L.SUSPEND_ACCOUNT__C                     AS SUSPEND_ACCOUNT_BOOL
                , L.ACCOUNT_SUSPENDED__C                   AS ACCOUNT_SUSPENDED_REASON
                , L.NAME                                   AS LICENSE_NAME
                , L.PRODUCTION__C                          AS C1_PRODUCTION_BOOL -- not sure this meaningful!
                , CASE
                         WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA COMPOSER'
                           THEN 1
                         WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'SALESFORCE CPQ: CONGA QUOTES'
                           THEN 2   
                         WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA INVOICE GENERATION'  
                           THEN 3
                         ELSE 4  
                  END                                      AS PACKAGE_SORT
                , CASE  
                    WHEN UPPER(L.SFLMA__LICENSE_STATUS__C) = 'ACTIVE'
                     AND EXPIRATION_DATE_STRING <> 'EXPIRED' 
                      THEN 0
                    WHEN UPPER(L.SFLMA__LICENSE_STATUS__C) = 'ACTIVE'
                      THEN 1  
                   ELSE 2 
                  END                                      AS STATUS_SORT                 
                , CASE  
                    WHEN UPPER(ORG_STATUS) IN ('DELETED', 'PENDING_DELETE', 'SUSPENDED')
                      OR SUSPEND_ACCOUNT__C = TRUE
                      THEN 3          
                    WHEN UPPER(ORG_STATUS) = 'ACTIVE'
                      THEN 0
                    WHEN UPPER(ORG_STATUS) IN ('FREE', 'SIGNING_UP')
                      THEN 1
                   ELSE 2 
                  END                                      AS ORG_STATUS_SORT  
                , ROW_NUMBER () OVER (PARTITION BY L.CUSTOMER_ORG_ID__C, P.PRODUCT ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, ORG_STATUS_SORT ASC, PACKAGE_SORT ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PRODUCT        
                , ROW_NUMBER () OVER (PARTITION BY L.CUSTOMER_ORG_ID__C, L.SFLMA__PACKAGE__C ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, ORG_STATUS_SORT ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PACKAGE_ID
                , ACTIVITY_DATE                            AS LAST_ACTIVITY_DATE
                , L.LASTMODIFIEDDATE                       AS CUSTOM_LAST_MOD
                , NULL                                     AS OLDCRM_ACCOUNT_ID
                , L.CONGA_ENFORCE_LMA__C
                , L.CONGA_ENFORCE_USER_MANAGEMENT__C 
                , CASE
                    WHEN L.CONGA_ENFORCE_LMA__C = false
                     AND L.CONGA_LICENSES__C is not null
                     AND L.CONGA_LICENSES__C = L.SFLMA__SEATS__C
                      THEN 'SAME'
                    WHEN L.CONGA_ENFORCE_LMA__C = false
                     AND L.CONGA_LICENSES__C is not null
                     AND L.CONGA_LICENSES__C <> L.SFLMA__SEATS__C
                      THEN 'REPLACED'                  
                   ELSE 'NA'
                  END AS ORG_SEATS_OVERRIDE 
                , CASE
                    WHEN L.CONGA_ENFORCE_LMA__C = false
                     AND L.CONGA_LICENSES__C is not null              
                      THEN L.SFLMA__SEATS__C
                   ELSE NULL
                  END AS LMA_ORIGINAL_SEATS         
        FROM                           license_with_orgs_override L
        LEFT OUTER JOIN        APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING P
                          ON L.SFLMA__PACKAGE__C = P.PACKAGE_ID
        LEFT JOIN                      APTTUS_DW.SF_CONGA1_1.SFLMA__PACKAGE__C PK 
                          ON L.SFLMA__PACKAGE__C = PK.ID                                                                                               
        WHERE L.ISDELETED = FALSE 
          AND L.SFLMA__PACKAGE__C  is not null 
          AND L.CUSTOMER_ORG_ID__C is not null
UNION  
        SELECT  'Apttus1.0' AS CRM_SOURCE
                , 'CLMCPQ Apttus1.0' as DATA_SOURCE        
                , L.ID as LICENSE_ID
                , L.SFLMA__SUBSCRIBER_ORG_ID__C                 AS CUSTOMER_ORG
                , NULL                                          AS CUSTOMER_ORG_18
                , substring(L.SFLMA__SUBSCRIBER_ORG_ID__C,1,15) AS CUSTOMER_ORG_15                  
                , P.PRODUCT_FAMILY                          AS PRODUCTFAMILY
                , P.PRODUCT
                , L.PACKAGE_NAME__C                         AS PACKAGE_NAME
                , NULL                                      AS PACKAGE_NAMEFX                
                , L.SFLMA__PACKAGE__C                       AS PACKAGE_ID
                , P.LMA_PACKAGE_ID                          AS PACKAGE_ID_AA
                , L.SFLMA__PACKAGE_VERSION__C               AS PACKAGE_VERSION_ID
                , CUSTOMER_ORG_15 || '-' || PACKAGE_ID      AS ORG_PACKAGE
                , L.SFLMA__LICENSE_STATUS__C                AS STATUS
                , COALESCE(SFLMA__ORG_STATUS__C, 'Unknown') AS ORG_STATUS 
                , M.A1_ACCOUNT_ID                           AS ACCOUNT_ID
                , L.ACCOUNT_NAME__C                         AS ACCOUNT_NAME  
                , L.SFLMA__IS_SANDBOX__C                    AS IS_SANDBOX
                , NULL                                      AS PREDICTED_PACKAGE_NAMESPACE  
                , CASE
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
                      THEN 'Site'
                   ELSE 'Seats'
                  END                                       AS LICENSE_SEAT_TYPE    
                , CASE
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
                      THEN NULL
                    WHEN L.SFLMA__SEATS__C > 0
                     THEN L.SFLMA__SEATS__C
                   ELSE 0               
                  END                                       AS SEATS
                , CASE 
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
                     AND L.SFLMA__USED_LICENSES__C > 1
                      THEN L.SFLMA__USED_LICENSES__C 
                   -- WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'                     
                   --   THEN NULL                
                   ELSE coalesce(L.SFLMA__USED_LICENSES__C, 0)
                  END                                       AS USED_LICENSES           
                , L.SFLMA__INSTALL_DATE__C                  AS INSTALL_DATE
                , CASE
                    WHEN UPPER(L.SFLMA__EXPIRATION_DATE__C) <> 'DOES NOT EXPIRE'
                      THEN to_Date(L.SFLMA__EXPIRATION__C)
                   else NULL   
                  END                                       AS EXPIRATION_DATE
                , CASE
                    WHEN UPPER(L.SFLMA__EXPIRATION_DATE__C) = 'DOES NOT EXPIRE' 
                     AND L.SFLMA__LICENSE_STATUS__C NOT IN ('Uninstalled')
                      THEN UPPER(L.SFLMA__EXPIRATION_DATE__C)
                    WHEN L.SFLMA__LICENSE_STATUS__C IN ('Uninstalled')
                      THEN 'UNINSTALLED'
                    WHEN EXPIRATION_DATE IS NOT NULL
                       AND CURRENT_DATE >= EXPIRATION_DATE
                           THEN 'EXPIRED'
                    WHEN UPPER(L.SFLMA__EXPIRATION_DATE__C) <> 'DOES NOT EXPIRE'            
                      THEN 'SET TO EXPIRE'  
                    ELSE 'EXPIRATION UNKNOWN'
                  END                                       AS EXPIRATION_DATE_STRING    
                , NULL                                      AS UNINSTALL_DATE -- CLMCPQ does appear to have an uninstall date
                , CASE 
                    WHEN EXPIRATION_DATE IS NOT NULL
                       AND CURRENT_DATE >= EXPIRATION_DATE
                       AND L.SFLMA__LICENSE_STATUS__C NOT IN ('Uninstalled')
                      THEN COALESCE(DATEDIFF(MONTH, L.SFLMA__INSTALL_DATE__C, CURRENT_DATE),0)               
                    WHEN L.SFLMA__LICENSE_STATUS__C NOT IN ('Uninstalled') 
                      THEN COALESCE(DATEDIFF(MONTH, L.SFLMA__INSTALL_DATE__C, CURRENT_DATE),0) 
                    ELSE 0
                  END AS MONTHS_INSTALLED    
                , CASE 
                    WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL AND MONTHS_INSTALLED > 12
                      THEN 'INSTALLED FOR ' || DATEDIFF(YEAR,  L.SFLMA__INSTALL_DATE__C, CURRENT_DATE) || ' YEARS'
                    WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL
                      THEN 'INSTALLED FOR ' || MONTHS_INSTALLED || ' MONTHS'  
                    ELSE 'INSTALL DATE NOT KNOWN'  
                  END                                      AS INSTALL_DATE_STRING  
                , NULL                                     AS SUSPEND_ACCOUNT_BOOL
                , NULL                                     AS ACCOUNT_SUSPENDED_REASON
                , L.NAME                                   AS LICENSE_NAME
                , NULL                                     AS C1_PRODUCTION_BOOL 
                , NULL                                     AS PACKAGE_SORT
                , CASE  
                    WHEN UPPER(L.SFLMA__LICENSE_STATUS__C) = 'ACTIVE'
                     AND EXPIRATION_DATE_STRING <> 'EXPIRED' 
                      THEN 0
                    WHEN UPPER(L.SFLMA__LICENSE_STATUS__C) = 'ACTIVE'
                      THEN 1  
                   ELSE 2 
                  END                                      AS STATUS_SORT               
                , CASE  
                    WHEN UPPER(ORG_STATUS) IN ('DELETED', 'PENDING_DELETE', 'SUSPENDED')
        --              OR SUSPEND_ACCOUNT__C = TRUE
                      THEN 3          
                    WHEN UPPER(ORG_STATUS) = 'ACTIVE'
                      THEN 0
                    WHEN UPPER(ORG_STATUS) IN ('FREE', 'SIGNING_UP')
                      THEN 1
                   ELSE 2 
                  END                                      AS ORG_STATUS_SORT  
                , ROW_NUMBER () OVER (PARTITION BY L.SFLMA__SUBSCRIBER_ORG_ID__C, P.PRODUCT ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, ORG_STATUS_SORT ASC, PACKAGE_NAME ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PRODUCT        
                , ROW_NUMBER () OVER (PARTITION BY L.SFLMA__SUBSCRIBER_ORG_ID__C, L.SFLMA__PACKAGE__C ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, ORG_STATUS_SORT ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PACKAGE_ID
                , ACTIVITY_DATE                            AS LAST_ACTIVITY_DATE
                , CUSTOM_LAST_MODIFIED_DATE__C             AS CUSTOM_LAST_MOD
                , L.SFLMA__ACCOUNT__C                      AS OLDCRM_ACCOUNT_ID
                , 1::BOOLEAN                               AS CONGA_ENFORCE_LMA__C
                , 1::BOOLEAN                               AS CONGA_ENFORCE_USER_MANAGEMENT__C                
                , 'NA'                                     AS ORG_SEATS_OVERRIDE 
                , NULL                                     AS LMA_ORIGINAL_SEATS 
        FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT L
        LEFT OUTER JOIN        APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING P
                          ON L.SFLMA__PACKAGE__C = P.PACKAGE_ID                 
        LEFT OUTER JOIN        APTTUS_DW.PRODUCT.CLMCPQ_A1_ACCOUNT_MAPPING M
                          ON L.SFLMA__ACCOUNT__C = M.CLMCPQ_ACCOUNT_ID                  
        WHERE L.ISDELETED = FALSE  
          AND L.SFLMA__SUBSCRIBER_ORG_ID__C is not null
          AND L.SFLMA__PACKAGE__C  is not null      
;                  