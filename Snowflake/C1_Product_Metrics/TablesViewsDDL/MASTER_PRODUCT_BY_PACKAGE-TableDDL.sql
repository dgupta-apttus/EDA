
CREATE TABLE APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_PACKAGE
        ( PACKAGE_ID   VARCHAR(16777216)
        , PRODUCT1      VARCHAR(16777216)
        )
; 

ALTER TABLE APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_PACKAGE rename column PRODUCT1 to PRODUCT;

/*
--delete from  APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_PACKAGE; 

INSERT INTO APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_PACKAGE 
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
           THEN 'Conga Billing'
         WHEN PRODUCT = 'Order Management'
           THEN 'Conga Order Management'                   
        ELSE PRODUCT 
       END as PRODUCT    
     -- NULL as         
FROM APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY
UNION 
SELECT 'a0T5000000J4HMnEAN', 'Conga Approvals'
;
*/