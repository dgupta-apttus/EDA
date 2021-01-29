
--DROP VIEW APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_ASSET_MAPPING;

CREATE OR REPLACE VIEW APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_ASSET_MAPPING
COMMENT = 'C2 product '
AS 
WITH natural_product2 as (
        SELECT 'Apttus1.0' AS CRM
             , ID AS PRODUCT_ID
             , NAME AS PRODUCT_NAME
             , FAMILY AS PRODUCT2_LINE 
        FROM  APTTUS_DW.SF_PRODUCTION.PRODUCT2
UNION
        SELECT 'Conga1.0' AS CRM
             , ID AS PRODUCT_ID
             , NAME AS PRODUCT_NAME
             , PRODUCT_LINE__C AS PRODUCT2_LINE
        FROM  APTTUS_DW.SF_CONGA1_1.PRODUCT2           
)
-- main
        SELECT A.PRODUCT_ID
             , A.PRODUCT_NAME
             , COALESCE(B.PRODUCT, 'Not Assigned') AS PRODUCT 
             , COALESCE(C.PRODUCT_LINE, 'Not Assigned') AS PRODUCT_LINE
             , COALESCE(C.PRODUCT_FAMILY, 'Not Assigned') AS PRODUCT_FAMILY 
             , COALESCE(A.PRODUCT2_LINE, 'Not Assigned') AS PRODUCT2_LINE
             , A.CRM 
        FROM                         natural_product2 A
        LEFT OUTER JOIN              APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_PRODUCTID B
                         ON A.PRODUCT_ID = B.PRODUCT_ID
        LEFT OUTER JOIN              APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_HIERARCHY C
                         ON B.PRODUCT = C.PRODUCT              
;
