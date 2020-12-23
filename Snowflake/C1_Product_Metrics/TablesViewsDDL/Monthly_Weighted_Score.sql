/*
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."Monthly_Weigted_Score"
COMMENT = 'Produce weighted score by ACV
'
AS*/
with average_score_per_acc_prod as (
        SELECT 	CRM
                , "Account ID"
                , REPORT_DATE
                , PRODUCT
                , AVG(ADOPTION_V1) AS ADOPTION_V1_AVG
                , COUNT(*) AS PRODUCT_LIC_COUNT 
       FROM APTTUS_DW.PRODUCT."Monthly_Score_Package_Org"
       WHERE "Active License Count" > 0
         AND "License Seat Type" IN ('Seats','Site')
       GROUP BY CRM
                , "Account ID"
                , REPORT_DATE
                , PRODUCT    
)
, join_asset_perc_to_score as (
        SELECT A.CRM
             , A."Account ID"
             , A."Product"
             , A."Report Date"
             , A."Product Percentage"
             , COALESCE(B.ADOPTION_V1_AVG, 0.0)::NUMBER(4,1) AS ADOPTION_V1_AVG
             , COALESCE(B.PRODUCT_LIC_COUNT, 0.0) AS PRODUCT_LIC_COUNT
        FROM                 APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_PERC_HIST_FL A
        LEFT OUTER JOIN      average_score_per_acc_prod B
                         ON  A."Account ID" = B."Account ID"
                         AND A.CRM = B.CRM
                         AND A."Product" = B.PRODUCT
                         AND A."Report Date" = B.REPORT_DATE
-- test
--where A."Account ID" = '0015000000ceqa0AAA'
-- test                     
)
, maths as (
        SELECT CRM
             , "Account ID"
             , "Product"
             , "Report Date"
             , "Product Percentage"
             , ADOPTION_V1_AVG
             , PRODUCT_LIC_COUNT
             , (("Product Percentage"/100) * ADOPTION_V1_AVG)::NUMBER(9,3) AS WEIGHTED
       FROM join_asset_perc_to_score      
)
        SELECT SUM(WEIGHTED) AS WEIGHTED_ADOPTION_V1
             , COUNT(*) AS PRODUCT_COUNT 
             , SUM(PRODUCT_LIC_COUNT) AS LICENSE_COUNT
             , CRM 
             , "Account ID"
             , "Report Date" 
        FROM maths
        GROUP BY CRM 
             , "Account ID"
             , "Report Date"                     
;
