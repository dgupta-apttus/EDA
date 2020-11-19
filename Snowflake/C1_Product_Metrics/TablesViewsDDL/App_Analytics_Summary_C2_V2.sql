CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.APP_ANALYTICS_SUMMARY_C2
COMMENT = 'build out app analytics summary joined for C1 and A1
'
AS 
        SELECT 'Conga1.0' AS CRM_SOURCE
             , MONTH
             , ORGANIZATION_ID as CUSTOMER_ORG_15
             , E.CUSTOMER_ORG_18
             , E.ACCOUNT_ID 
             , COALESCE(F.ACCOUNTID_18__C, 'Not Found') AS C2_ACCOUNT
             , COALESCE(F.ACCOUNT_NAME, 'Name Not Found') AS ACCOUNT_NAME
             , F.ACCOUNT_NAME AS LMA_ACCOUNT_NAME
             , A.PACKAGE_ID AS LMA_PACKAGE_ID
             , B.ID AS PACKAGE_ID
             , Coalesce(B.NAME, A.PACKAGE_ID) as PACKAGE_NAME
             , D.PRODUCTFAMILY
             , D.PRODUCT 
             , D.PACKAGENAME  
             , C.PRODUCT_LINE    
             , E.LICENSE_ID  
             , COALESCE(E.LICENSE_NAME, 'License Not Found') AS LICENSE_NAME  
             , E.STATUS
             , E.ORG_STATUS 
             , E.EXPIRATION_DATE_STRING  
             , E.IS_SANDBOX                                        
             , MANAGED_PACKAGE_NAMESPACE
             , CUSTOM_ENTITY
             , CUSTOM_ENTITY_TYPE
             , USER_ID_TOKEN
             , USER_TYPE
             , NUM_CREATES
             , NUM_READS
             , NUM_UPDATES
             , NUM_DELETES
             , NUM_VIEWS 
        FROM                        APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY_C1 A
        LEFT OUTER JOIN             APTTUS_DW.SF_CONGA1_1.SFLMA__PACKAGE__C B
                          ON A.PACKAGE_ID = substr(B.SFLMA__PACKAGE_ID__C, 1,15)  
        LEFT OUTER JOIN             APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE C
                          ON B.NAME = C.PACKAGE_NAME
        LEFT OUTER JOIN             APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY D
                          ON B.ID = D.PACKAGEID
        LEFT OUTER JOIN             APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT E
                          ON  A.ORGANIZATION_ID = E.CUSTOMER_ORG_15
                          AND B.ID = E.PACKAGE_ID  
                          AND E.CRM_SOURCE = 'Conga1.0'  
                          AND E.SELECT1_FOR_PACKAGE_ID = 1 -- get best reord per package if there is more than one
        LEFT OUTER JOIN             APTTUS_DW.SF_PRODUCTION."Account_C2" F
                  ON  E.ACCOUNT_ID = F.ACCOUNTID_18__C 
                  AND F.SOURCE = 'CONGA1.0'                                                                 
UNION ALL 
        SELECT 'Apttus1.0' AS CRM_SOURCE
             , MONTH
             , ORGANIZATION_ID as CUSTOMER_ORG_15
             , E.CUSTOMER_ORG_18
             , E.ACCOUNT_ID
             , COALESCE(G.ACCOUNTID_18__C, 'Not Found') AS C2_ACCOUNT             
             , COALESCE(G.ACCOUNT_NAME, E.ACCOUNT_NAME, 'Name Not Found') as ACCOUNT_NAME 
             , E.ACCOUNT_NAME as LMA_ACCOUNT_NAME
             , A.PACKAGE_ID AS LMA_PACKAGE_ID
             , B.ID AS PACKAGE_ID
             , Coalesce(B.NAME, A.PACKAGE_ID) as PACKAGE_NAME
             , D.PRODUCTFAMILY
             , D.PRODUCT                                  
             , D.PACKAGENAME  
             , D.PRODUCT as PRODUCT_LINE    
             , E.LICENSE_ID  
             , COALESCE(E.LICENSE_NAME, 'License Not Found') AS LICENSE_NAME  
             , E.STATUS
             , E.ORG_STATUS 
             , E.EXPIRATION_DATE_STRING  
             , E.IS_SANDBOX                                                           
             , MANAGED_PACKAGE_NAMESPACE
             , CUSTOM_ENTITY
             , CUSTOM_ENTITY_TYPE
             , USER_ID_TOKEN
             , USER_TYPE
             , NUM_CREATES
             , NUM_READS
             , NUM_UPDATES
             , NUM_DELETES
             , NUM_VIEWS 
        FROM                        APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY A
        LEFT OUTER JOIN             APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__PACKAGE__C B
                          ON A.PACKAGE_ID = substr(B.SFLMA__PACKAGE_ID__C, 1,15)  
        LEFT OUTER JOIN             APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY D
                          ON B.ID = D.PACKAGEID
        LEFT OUTER JOIN             APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT E
                          ON  A.ORGANIZATION_ID = E.CUSTOMER_ORG_15
                          AND B.ID = E.PACKAGE_ID  
                          AND E.CRM_SOURCE = 'Apttus1.0'  
                          AND E.SELECT1_FOR_PACKAGE_ID = 1 -- get best reord per package if there is more than one                          
        LEFT OUTER JOIN             APTTUS_DW.PRODUCT.CLMCPQ_A1_ACCOUNT_MAPPING F                    
                          ON  E.ACCOUNT_ID = F.CLMCPQ_ACCOUNT_ID                                          
        LEFT OUTER JOIN             APTTUS_DW.SF_PRODUCTION."Account_C2" G
                          ON F.A1_ACCOUNT_ID = G.ACCOUNTID_18__C                        
;
