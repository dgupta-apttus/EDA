select distinct PRODUCT as PRODUCT_LINE 
from APTTUS_DW.PRODUCT."Master_Package_List"
;

select * from APTTUS_DW.PRODUCT."Master_Package_List";


select *
from   APTTUS_DW.SF_PRODUCTION.LMA_Product_Package_Mapping__c
;

WITH A_PRODUCTS_ON_ASSETS AS ( -- 442 distinct product ids
        SELECT distinct APTTUS_CONFIG2__PRODUCTID__C as PRODUCT_ID, NAME as ASSET_NAME                    
        FROM                     APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY A
)
, C_PRODUCTS_ON_ASSETS AS ( --470 distinct product ids
        select distinct PRODUCT2ID AS PRODUCT_ID, NAME as ASSET_NAME
        FROM   APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
)
, product_by_asset_name as (
        SELECT  ASSET_NAME
             , listagg(distinct C2_PRODUCTFAMILY, ', ') within group (ORDER BY  C2_PRODUCTFAMILY) AS PRODUCT_LINE
        FROM APTTUS_DW.SF_PRODUCTION.C2_ASSETMAPPING_V1	
        group by ASSET_NAME
)
, product_line AS (
        select A.PRODUCT_ID
             , A.ASSET_NAME
             , B.NAME AS PRODUCT_NAME
             , COALESCE(B.FAMILY, 'Unidentified Product')                   AS PRODUCT_LINE_P   
             , 'Apttus1.0' as CRM
        FROM                  A_PRODUCTS_ON_ASSETS A
        LEFT OUTER JOIN       APTTUS_DW.SF_PRODUCTION.PRODUCT2 B
                         ON A.PRODUCT_ID = B.ID                                      
UNION
        select C.PRODUCT_ID
             , C.ASSET_NAME
             , D.NAME AS PRODUCT_NAME
             , COALESCE(D.PRODUCT_LINE__C, 'Unidentified Product')          AS PRODUCT_LINE_P
             , 'Conga1.0' as CRM
        FROM                 C_PRODUCTS_ON_ASSETS C  
        LEFT OUTER JOIN       APTTUS_DW.SF_CONGA1_1.PRODUCT2 D   
                         ON C.PRODUCT_ID = D.ID  
)
, result1 as (
        SELECT A.PRODUCT_LINE_P
             , B.PRODUCT_LINE as PRODUCT_LINE_A_LKUP
             , A.PRODUCT_ID
             , A.ASSET_NAME
             , A.PRODUCT_NAME
             , A.CRM 
        FROM                   product_line A
        left outer join        product_by_asset_name B
                          ON A.ASSET_NAME = B.ASSET_NAME
        ORDER BY A.PRODUCT_LINE_P, B.PRODUCT_LINE, A.ASSET_NAME
)
--        select * from result1
          select  distinct PRODUCT_NAME, PRODUCT_ID, null as NEW_FAMILY, PRODUCT_LINE_A_LKUP from result1   
--        select distinct PRODUCT_LINE_P, PRODUCT_LINE_A_LKUP from result1
--        select distinct PRODUCT_LINE_P from result1
--          SELECT distinct PRODUCT_LINE_A_LKUP from result1                        
;


CREATE TABLE APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_PACKAGE
        ( PACKAGE_ID   VARCHAR(16777216)
        , PRODUCT      VARCHAR(16777216)
        )
; 

INSERT INTO  
SELECT PACKAGEID  as PACKAGE_ID
--     , PACKAGENAME  
--     , PRODUCT
     , CASE
         WHEN PRODUCT = 'Approvals'
           THEN 'Conga Approvals'
         WHEN PRODUCT = 'Billing'
           THEN 'Conga Billing'
         WHEN PRODUCT = 'CLM'
           THEN 'Conga CLM'     
         WHEN PRODUCT = 'CPQ'
           THEN 'Conga CPQ'  
         WHEN PRODUCT = 'N/A'
           THEN 'Other'
         WHEN PRODUCT = 'Digital Commerce'
           THEN 'Conga Digital Commerce'
         WHEN PRODUCT = 'X-Author Enterprise'
           THEN 'Conga X-Author Enterprise'   
         WHEN PRODUCT = 'Revenue Recognition'
           THEN 'Conga Revenue Management'                 
        ELSE PRODUCT 
       END as PRODUCT    
     -- NULL as         
FROM APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY
UNION 
SELECT 'a0T5000000J4HMnEAN', 'Conga Approvals'
;

/*
select * from product_by_asset_name


;

        select distinct
             , ID                                                         AS PRODUCT_ID
             , NAME                                                       AS PRODUCT_NAME
             , COALESCE(PRODUCT_LINE__C, 'Unidentified Product')          AS PRODUCT_LINE
        FROM  APTTUS_DW.SF_CONGA1_1.PRODUCT2
UNION
        select distinct
               ID                                                         AS PRODUCT_ID
             , NAME                                                       AS PRODUCT_NAME
             , COALESCE(FAMILY, 'Unidentified Product')                   AS PRODUCT_LINE                
        FROM  APTTUS_DW.SF_PRODUCTION.PRODUCT2           
;
*/