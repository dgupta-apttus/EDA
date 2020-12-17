
CREATE OR REPLACE VIEW APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING
COMMENT = 'C2 product Package Mapping
 -- V1 - 2020-12-15 - Naveen
 --  2020-12-15 added pckage names and LMA type package ids (connection to App Analytics) -- Greg
'
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
        SELECT A.ID as PACKAGE_ID 
             , A.LMA_PACKAGE_ID
             , A.SFLMA__PACKAGE_ID__C as SFLMA_PACKAGE_ID18
             , A.PACKAGE_NAME 
             , COALESCE(B.PRODUCT, 'Not Assigned') AS PRODUCT 
             , COALESCE(C.PRODUCT_LINE, 'Not Assigned') AS PRODUCT_LINE
             , COALESCE(C.PRODUCT_FAMILY, 'Not Assigned') AS PRODUCT_FAMILY 
        FROM                         get_packages_both A
        LEFT OUTER JOIN              APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_PACKAGE B
                         ON A.ID = B.PACKAGE_ID  
        LEFT OUTER JOIN              APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_HIERARCHY C
                         ON B.PRODUCT = C.PRODUCT                                                                
;