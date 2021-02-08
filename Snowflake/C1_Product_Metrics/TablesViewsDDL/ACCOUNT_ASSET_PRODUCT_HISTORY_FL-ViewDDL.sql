
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY_FL
COMMENT = 'Add field labels for ACCOUNT_ASSET_PRODUCT_HISTORY
-- 2020/12/16  adjust to standard product hierarchy'
AS  
select ACCOUNTID                    AS "Account ID"
      , CRM                         AS "CRM" 
      , ASSET_ID                    AS "Asset ID"
      , ASSET_NAME                  AS "Asset Name"
      , REPORT_DATE                 AS "Report Date"
      , PRODUCT_ID                  AS "Product ID"
      , PRODUCT_NAME                AS "Product Name"
      , PRODUCT                     AS "Product"
      , PRODUCT_LINE                AS "Product Line"
      , PRODUCT_FAMILY              AS "Product Family"  
      , PRODUCT2_LINE               AS "Product Table Line" 
      , ORG_SOURCE                  AS "Org Source"
      , CUSTOMER_ORG                AS "Customer Org" 
      , QUANTITY                    AS "Quantity"
      , ACV                         AS "ACV"
      , ACV_ON_ACCOUNT              AS "ACV on Account" 
      , ASSET_STATUS                AS "Asset Status"
      , START_DATE                  AS "Asset Start Date"
      , END_DATE                    AS "Asset End Date" 
      , ORIGINAL_CURRENCY           AS "Currency"  
      , ORIGINAL_ACV                AS "Local ACV"
FROM APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY
;