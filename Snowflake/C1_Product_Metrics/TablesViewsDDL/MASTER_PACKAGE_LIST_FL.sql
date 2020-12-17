
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.MASTER_PACKAGE_LIST_FL
COMMENT = 'Field Label version of Master Package List
-- 2020/12/16 -- now points to SF_PRODUCTION	MASTER_PRODUCT_ASSET_MAPPING
'
AS 
        SELECT PACKAGE_ID                               AS "Package ID"
             , COALESCE(LMA_PACKAGE_ID, 'Unknown')      AS "LMA Package ID"
             , COALESCE(SFLMA_PACKAGE_ID18, 'Unknown')  AS "LMA Package ID18"
             , COALESCE(PACKAGE_NAME, 'Unknown')        AS "Package Name" 
             , COALESCE(PRODUCT, 'Unknown')             AS "Product"
             , COALESCE(PRODUCT_LINE, 'Unknown')        AS "Product Line"
             , COALESCE(PRODUCT_FAMILY, 'Unknown')      AS "Product Family"   
        FROM                APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING              
;