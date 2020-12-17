
/*version 1
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_PERC_HIST_FL
COMMENT = 'Add field labels for Percentage of Product Spend per Account'
AS  
SELECT ACCOUNTID  AS "Account ID"
     , CRM 
     , PRODUCT AS "Product"
     , REPORT_DATE As "Report Date"
     , QUANTITY AS "Quantity"
     , ORG_COUNT AS "Organization Count"
     , PRODUCT_ID_COUNT AS "Unique Product ID Count"
     , PRODUCT_ACV AS "Per Product ACV"
     , ACV_ON_ACCOUNT AS "Account AVC"
     , PRODUCT_ACV_PERCENTAGE as "Product Percentage"
     , POSITIVE_ACCOUNT_ACV as "Positive ACV used for Calculation"
FROM APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_PERC_HISTORY
;
*/
-- version 2 coming from table
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_PERC_HIST_FL
COMMENT = 'Add field labels for Percentage of Product Spend per Account
-- 2020/12/16 adjust to standard product hierarchy - gdw
'
AS  
WITH sum_product_c1_pos as (
        select ACCOUNTID
             , CRM
             , PRODUCT
             , REPORT_DATE
             , SUM(ACV) AS ACV
             , count(*) AS POS_ROWS 
        FROM  APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY   
        GROUP BY ACCOUNTID
               , CRM
               , PRODUCT
               , REPORT_DATE
        HAVING SUM(ACV) > 0
           AND MAX(ACV_ON_ACCOUNT) > 0  
)
, sum_product_c1_all as (
        select ACCOUNTID
             , CRM
             , PRODUCT
             , REPORT_DATE
             , SUM(QUANTITY) as QUANTITY
             , COUNT(distinct CUSTOMER_ORG) as ORG_COUNT
             , COUNT(distinct PRODUCT_ID) as PRODUCT_ID_COUNT 
             , SUM(ACV) AS ACV
             , MAX(ACV_ON_ACCOUNT) as ACV_ON_ACCOUNT 
             , count(*) AS ASSET_ITEMS                             
        FROM  APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY
        GROUP BY ACCOUNTID
               , CRM
               , PRODUCT
               , REPORT_DATE
)
--, inner1 as (
        SELECT A.ACCOUNTID                                           AS "Account ID"
             , A.CRM
             , A.PRODUCT                                             AS "Product"
             , A.REPORT_DATE                                         AS "Report Date"
             , A.QUANTITY                                            AS "Quantity"
             , A.ORG_COUNT                                           AS "Organization Count"
             , A.PRODUCT_ID_COUNT                                    AS "Unique Product ID Count"
             , A.ASSET_ITEMS                                         AS "Asset Items"
             , ROUND(A.ACV, 2)                                       AS "Per Product ACV"
             , ROUND(A.ACV_ON_ACCOUNT, 2)                            AS "Account AVC" 
             , CASE 
                 WHEN B.ACV is not null 
                   THEN ROUND((B.ACV*100)/(SUM(B.ACV) OVER (PARTITION BY B.ACCOUNTID)), 1)
                ELSE 0
               END                                                   AS "Product Percentage"
             , COALESCE(ROUND(SUM(B.ACV) OVER (PARTITION BY B.ACCOUNTID), 2), 0) AS "Positive ACV used for Calculation"  -- this is for precentage calcs only here for check your work
             , B.POS_ROWS                                            AS "Items in Positive ACV"
        FROM                    sum_product_c1_all A
        LEFT OUTER JOIN         sum_product_c1_pos B
                          ON  A.ACCOUNTID = B.ACCOUNTID
                          AND A.CRM = B.CRM
                          AND A.PRODUCT = B.PRODUCT   
                          AND A.REPORT_DATE = B.REPORT_DATE
;                          