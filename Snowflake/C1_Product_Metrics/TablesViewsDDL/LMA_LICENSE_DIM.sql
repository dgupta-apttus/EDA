
-- needs label names!

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.LMA_LICENSE_DIM 
COMMENT = 'Attributes of LICENSE_ID'
AS 
        select  LICENSE_ID                    AS "License ID"
              , CRM_SOURCE                    AS CRM   
              , DATA_SOURCE                   AS "Data Source"
              , CUSTOMER_ORG                  AS "Customer Org"
              , LICENSE_NAME || '-' || SUBSTR(CRM_SOURCE, 1, 1)  AS "License Name"
              , LICENSE_SEAT_TYPE             AS "License Seat Type"
              , PACKAGE_ID                    AS "Package ID"
              , PACKAGE_ID_AA                 AS "Package ID AA" 
              , CASE 
                  WHEN IS_SANDBOX = true
                    THEN 'Sandbox'
                  WHEN IS_SANDBOX = false
                   AND (UPPER(STATUS) <> 'ACTIVE'
                        OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
                       )
                    THEN 'Not Production'
                  WHEN UPPER(STATUS) = 'ACTIVE'
                   AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
                   AND IS_SANDBOX = false
                   AND ACCOUNT_ID is null
                    THEN 'Active w/o Acc'
                  WHEN UPPER(STATUS) = 'ACTIVE'
                   AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
                   AND IS_SANDBOX = false
                    THEN 'Active'                     
                 ELSE 'Unknown'
                END                              AS "License Status"   
              , STATUS                           AS "LMA Status"
              , ORG_STATUS                       AS "Org Status" 
              , IS_SANDBOX                       AS "Is Sandbox" 
              , INSTALL_DATE                     AS "Install Date"
              , UNINSTALL_DATE                   AS "Uninstall Date"
              , MONTHS_INSTALLED                 AS "Months Installed"
              , INSTALL_DATE_STRING              AS "Install Date Descr" 
              , EXPIRATION_DATE                  AS "Expire Date"
              , EXPIRATION_DATE_STRING           AS "Expire Date Descr"
              , ACCOUNT_ID                       AS "Account ID" 
              , OLDCRM_ACCOUNT_ID                AS "Account ID(Old CRM)"
              , CONGA_ENFORCE_LMA__C             AS "Enforce LMA?"                                                            
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        where LAST_ACTIVITY_DATE >= (CURRENT_DATE()-366)
           OR (     LAST_ACTIVITY_DATE < (CURRENT_DATE()-366)
               AND  UPPER(STATUS) = 'ACTIVE'
               AND  EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED', 'EXPIRED') 
               AND  ORG_STATUS NOT IN ('DELETED')
              )            
;
