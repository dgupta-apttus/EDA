
need to add ID as ASSET ID from the Snapshot histories 

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
;        select ACCOUNTID
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
WHERE ACCOUNTID = '0011U00000D92bqQAB'  
  and PRODUCT = 'Conga CLM'
        GROUP BY ACCOUNTID
               , CRM
               , PRODUCT
               , REPORT_DATE
;                  
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
                          
WHERE A.ACCOUNTID = '0011U00000D92bqQAB'  
  and A.PRODUCT = 'Conga CLM' 
;

select * from APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY
WHERE ACCOUNTID = '0011U00000D92bqQAB' 
  and PRODUCT = 'Conga CLM' 
;                        
select *
FROM APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY_FL
WHERE "Account ID" = '0011U00000D92bqQAB' 
  and "Product" = 'Conga CLM' 