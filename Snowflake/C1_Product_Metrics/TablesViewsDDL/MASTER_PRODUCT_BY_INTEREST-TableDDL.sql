CREATE TABLE APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_INTEREST
        (  PRODUCT_INTEREST  VARCHAR(16777216)  
         , PRODUCT1          VARCHAR(16777216)
        )
; 


INSERT INTO APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_INTEREST
SELECT PRODUCT_FAMILY AS PRODUCT_INTEREST
     , C2_PRODUCT_FAMILY AS PRODUCT1 
FROM APTTUS_DW.SF_PRODUCTION.C2_PRODUCTFAMILY_MAPPING	
;

ALTER TABLE APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_BY_INTEREST rename column PRODUCT1 to PRODUCT;

 
