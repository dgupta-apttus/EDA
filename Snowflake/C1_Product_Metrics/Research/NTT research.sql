



        SELECT SFLMA__SUBSCRIBER_ORG_ID__C
             , P.PRODUCT
             , SFLMA__LICENSE_STATUS__C 
             , CASE
                WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License' 
                 THEN 0  
                ELSE SFLMA__SEATS__C
               END AS PURCHASED_SEATS     
             , SFLMA__USED_LICENSES__C
             , NAME
             , SFLMA__EXPIRATION_DATE__C
             , SFLMA__IS_SANDBOX__C
             , P.PRODUCT_FAMILY                          AS PRODUCTFAMILY
             , L.PACKAGE_NAME__C                         AS PACKAGE_NAME            
             , L.SFLMA__PACKAGE__C                       AS PACKAGE_ID
             , P.LMA_PACKAGE_ID                          AS PACKAGE_ID_AA
             , M.A1_ACCOUNT_ID                           AS ACCOUNT_ID
             , ACCOUNTID__C
             , CASE
                    WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
                      THEN 'Site'
                   ELSE 'Seats'
               END                                       AS LICENSE_SEAT_TYPE
        FROM                   APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__LICENSE__C L
        LEFT OUTER JOIN        APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING P
                          ON L.SFLMA__PACKAGE__C = P.PACKAGE_ID                 
        LEFT OUTER JOIN        APTTUS_DW.PRODUCT.CLMCPQ_A1_ACCOUNT_MAPPING M
                          ON L.SFLMA__ACCOUNT__C = M.CLMCPQ_ACCOUNT_ID 
        WHERE SFLMA__SUBSCRIBER_ORG_ID__C = '00D10000000J1AM'
;


SELECT * 
FROM APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__LICENSE__C
WHERE ACCOUNT_ID__C = '0011U00000D8wXIQ'
;

SELECT * 
FROM APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__LICENSE__C
WHERE	SFLMA__SUBSCRIBER_ORG_ID__C = '00D10000000J1AM'
--00d10000000j1am -org
--0011U00000D8wXIQ - account
;
SELECT *
FROM APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
WHERE CUSTOMER_ORG_15 = '00d10000000j1am'
;

SELECT *
FROM APTTUS_DW.PRODUCT."Active_Licenses"
WHERE "Account Name" Like  'NTT%'
;

SELECT *
FROM APTTUS_DW.PRODUCT."Active_Licenses"
WHERE "Customer Org" = '00D10000000J1AM'
;