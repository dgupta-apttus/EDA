
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."Master_Package_List"
COMMENT = 'C2 packages unioned from source and Product Family Hiearchy added'
AS 
with get_packages_both as (
        select substr(SFLMA__PACKAGE_ID__C, 1,15) as LMA_PACKAGE_ID
             , SFLMA__PACKAGE_ID__C
             , ID  
             , NAME AS PACKAGE_NAME
        from  APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__PACKAGE__C 
union
        select substr(SFLMA__PACKAGE_ID__C, 1,15) as LMA_PACKAGE_ID
             , SFLMA__PACKAGE_ID__C
             , ID  
             , NAME AS PACKAGE_NAME
        from  APTTUS_DW.SF_CONGA1_1.SFLMA__PACKAGE__C 
)
        SELECT A.LMA_PACKAGE_ID
             , A.SFLMA__PACKAGE_ID__C as SFLMA_PACKAGE_ID18
             , A.ID as PACKAGEID 
             , A.PACKAGE_NAME 
             , B.PRODUCT
             , B.PRODUCTFAMILY
             , B.PRODUCTPILLAR       
             , C.PRODUCT_LINE AS PRODUCT_LINE_C1
        FROM                get_packages_both A
        LEFT OUTER JOIN     APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY B
                         ON A.ID = B.PACKAGEID    
        LEFT OUTER JOIN     APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_TWO C
                         ON A.ID = C.PACKAGE_ID                       
;
/*
-- test unique 2 ways
Select count(*), PACKAGEID
from  APTTUS_DW.PRODUCT."Master_Package_List"
group by PACKAGEID
having COUNT(*) > 1
;

Select count(*), LMA_PACKAGE_ID
from  APTTUS_DW.PRODUCT."Master_Package_List"
group by LMA_PACKAGE_ID
having COUNT(*) > 1
;
*/

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.MASTER_PACKAGE_LIST_FL
COMMENT = 'Field Label version of Master PAckage List'
AS 
        SELECT PACKAGEID                                AS "Package ID"
             , COALESCE(LMA_PACKAGE_ID, 'Unknown')      AS "LMA Package ID"
             , COALESCE(SFLMA_PACKAGE_ID18, 'Unknown')  AS "LMA Package ID18"
             , COALESCE(PACKAGE_NAME, 'Unknown')        AS "Package Name" 
             , COALESCE(PRODUCT, 'Unknown')             AS "Product Line"
             , COALESCE(PRODUCTFAMILY, 'Unknown')       AS "Product Family"   
             , COALESCE(PRODUCTPILLAR, 'Unknown')       AS "Product Pillar"
        FROM                APTTUS_DW.PRODUCT."Master_Package_List"              
;