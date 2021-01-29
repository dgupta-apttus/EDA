select count(*), LICENSE_NAME, CRM_SOURCE  --, LICENSE_ID
FROM APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
group by LICENSE_NAME, CRM_SOURCE --LICENSE_ID
having count(*) > 1
;

select count(*), SUBSTR(CRM_SOURCE, 1, 1) || '-' || LICENSE_NAME
FROM APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
group by SUBSTR(CRM_SOURCE, 1, 1) || '-' || LICENSE_NAME
having count(*) > 1
;

select * from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
;


CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.LMA_LICENSE_DIM 
COMMENT = 'Attributes of LICENSE_ID'
AS 
        select  LICENSE_ID
              , CRM_SOURCE                    AS CRM   
              , DATA_SOURCE 
              , CUSTOMER_ORG   
              , LICENSE_NAME || '-' || SUBSTR(CRM_SOURCE, 1, 1)  AS LICENSE_NAME
              , LICENSE_SEAT_TYPE
              , PACKAGE_ID
              , PACKAGE_ID_AA
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
                END                              AS COMPOSIT_LIC_STATUS   
              , STATUS
              , ORG_STATUS
              , IS_SANDBOX
              , INSTALL_DATE 
              , UNINSTALL_DATE
              , MONTHS_INSTALLED
              , INSTALL_DATE_STRING
              , EXPIRATION_DATE
              , EXPIRATION_DATE_STRING
              , ACCOUNT_ID
              , OLDCRM_ACCOUNT_ID
              , CONGA_ENFORCE_LMA__C                                                                        
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        where LAST_ACTIVITY_DATE >= (CURRENT_DATE()-366)
           OR (     LAST_ACTIVITY_DATE < (CURRENT_DATE()-366)
               AND  UPPER(STATUS) = 'ACTIVE'
               AND  EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED', 'EXPIRED') 
               AND  ORG_STATUS NOT IN ('DELETED')
              )            
;
