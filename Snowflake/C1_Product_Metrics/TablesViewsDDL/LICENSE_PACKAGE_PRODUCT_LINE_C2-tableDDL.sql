--DROP VIEW APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_C2;

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_C2
COMMENT = 'join out package definitions for C2'
AS 
with happening_AA_namespaces_C1 AS (
        select distinct A.MANAGED_PACKAGE_NAMESPACE              
                      , A.PACKAGE_ID AS LMA_PACKAGE_ID
                      , B.ID AS PACKAGE_ID
                      , B.NAME as PACKAGE_NAME
                      , A.CRM
        FROM                          APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY A
        LEFT OUTER JOIN               APTTUS_DW.SF_CONGA1_1.SFLMA__PACKAGE__C B
                                  ON A.PACKAGE_ID = substr(B.SFLMA__PACKAGE_ID__C, 1,15) 
        where A.CRM = 'Conga1.0'    
)
, happening_AA_namespaces_A1 AS (
        select distinct A.MANAGED_PACKAGE_NAMESPACE              
                      , A.PACKAGE_ID AS LMA_PACKAGE_ID
                      , B.ID AS PACKAGE_ID
                      , B.NAME as PACKAGE_NAME
                      , A.CRM
        FROM                          APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY A
        LEFT OUTER JOIN               APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__PACKAGE__C B
                                  ON A.PACKAGE_ID = substr(B.SFLMA__PACKAGE_ID__C, 1,15) 
        where A.CRM = 'Apttus1.0'    
)
        SELECT A.PACKAGE_NAME
             , A.PRODUCT_LINE
             , 'Conga1.0' AS CRM
             , E.PRODUCT
             , E.PRODUCTFAMILY
             , E.PRODUCTPILLAR                 
             , COALESCE(B.MANAGED_PACKAGE_NAMESPACE, 'Unknown') AS MANAGED_PACKAGE_NAMESPACE
             , COALESCE(B.LMA_PACKAGE_ID, substr(D.SFLMA__PACKAGE_ID__C, 1,15)) as LMA_PACKAGE_ID
             , COALESCE(B.PACKAGE_ID, C.PACKAGEID) as PACKAGE_ID
             , CASE 
                WHEN B.CRM is not null
                  THEN 1::BOOLEAN
                ELSE 0::BOOLEAN
               END AS AA_OCCURS   
        FROM                          APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE A
        LEFT OUTER JOIN               APTTUS_DW.PRODUCT.C1_MASTER_PRODUCT_FAMILY C
                            ON A.PACKAGE_NAME = C.PACKAGENAME
        LEFT OUTER JOIN               happening_AA_namespaces_C1 B
                            ON  A.PACKAGE_NAME = B.PACKAGE_NAME
                            AND C.PACKAGEID = B.PACKAGE_ID
        LEFT OUTER JOIN               APTTUS_DW.SF_CONGA1_1.SFLMA__PACKAGE__C D
                            ON D.ID = C.PACKAGEID      
        LEFT OUTER JOIN               APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY E
                            ON C.PACKAGEID = E.PACKAGEID                          
UNION ALL 
        SELECT A.PACKAGENAME AS PACKAGE_NAME
             , A.PRODUCT AS PRODUCT_LINE
             , 'Apttus1.0' as CRM
             , A.PRODUCT
             , A.PRODUCTFAMILY
             , A.PRODUCTPILLAR             
             , COALESCE(B.MANAGED_PACKAGE_NAMESPACE, 'Unknown') AS MANAGED_PACKAGE_NAMESPACE
             , COALESCE(B.LMA_PACKAGE_ID, substr(D.SFLMA__PACKAGE_ID__C, 1,15)) as LMA_PACKAGE_ID
             , COALESCE(B.PACKAGE_ID, A.PACKAGEID) as PACKAGE_ID
             , CASE 
                WHEN B.CRM is not null
                  THEN 1::BOOLEAN
                ELSE 0::BOOLEAN
               END AS AA_OCCURS   
        FROM                          APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY A
        LEFT OUTER JOIN               happening_AA_namespaces_A1 B
                            ON  A.PACKAGENAME = B.PACKAGE_NAME
                            AND A.PACKAGEID = B.PACKAGE_ID
        LEFT OUTER JOIN               APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__PACKAGE__C D
                            ON D.ID = A.PACKAGEID   
        WHERE A.PACKAGENAME like 'Apttus%'                      
;

-- snapshot this from time to time just in case manual snaps have a date at the end
create table APTTUS_DW.SNAPSHOTS.LICENSE_PACKAGE_PRODUCT_LINE_C2_SNAP_-- YYYYMMDD
AS
SELECT * FROM APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_C2;
