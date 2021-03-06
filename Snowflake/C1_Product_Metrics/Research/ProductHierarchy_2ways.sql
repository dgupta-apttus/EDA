
with PACKAGES as (
        SELECT PRODUCT, CRM_SOURCE, COUNT(DISTINCT ACCOUNT_ID) AS PACKAGE_ACCOUNT_COUNT
             --, SUM(ACTIVE_LICENSE_COUNT) as ACTIVE_LICENSES
        FROM APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY
        WHERE REPORTING_DATE = '2020-11-01'
          and ACTIVE_SEAT_TYPE NOT IN ('Not Production')
        GROUP BY PRODUCT, CRM_SOURCE
)
, ASSETS AS (
        SELECT PRODUCT, CRM, COUNT(DISTINCT ACCOUNTID) AS ASSET_ACCOUNT_COUNT
        FROM APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_PERC_HISTORY
        GROUP BY PRODUCT, CRM
)
        SELECT COALESCE(A.PRODUCT, B.PRODUCT) AS PRODUCT
             , COALESCE(A.CRM_SOURCE, B.CRM) AS CRM
             , A.PACKAGE_ACCOUNT_COUNT
--             , A.ACTIVE_LICENSES
             , B.ASSET_ACCOUNT_COUNT
        FROM                PACKAGES A
        FULL OUTER JOIN     ASSETS B
                         ON  A.PRODUCT = B.PRODUCT
                         AND A.CRM_SOURCE = B.CRM  
        order by 1, 2                   
;                          

select count(*), ASSET_NAME --, PRODUCT_FAMILY__C, PRODUCT_FAMILY_ADJ      
FROM APTTUS_DW.SF_PRODUCTION.C2_ASSETMAPPING_V1	
group by ASSET_NAME -- , PRODUCT_FAMILY__C, PRODUCT_FAMILY_ADJ
;

SELECT  ASSET_NAME
     , listagg(distinct C2_PRODUCTFAMILY, ', ') AS PRODUCT
FROM APTTUS_DW.SF_PRODUCTION.C2_ASSETMAPPING_V1	
group by ASSET_NAME
;