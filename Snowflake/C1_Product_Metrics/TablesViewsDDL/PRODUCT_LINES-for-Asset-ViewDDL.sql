


CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.PRODUCT_LINES
COMMENT = 'Add field labels for ACCOUNT_ASSET_PRODUCT_HISTORY
-- 2020/12/16 adjust to new product hierarchy - gdw
'
AS  
        SELECT distinct PRODUCT AS "Product"
        FROM APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_ASSET_MAPPING
        union 
        SELECT 'Unidentified Product' 
;

--CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.PRODUCT_LINES
--COMMENT = 'Add field labels for ACCOUNT_ASSET_PRODUCT_HISTORY'
--AS  
        select distinct ASSET_PRODUCT_LINE AS "Product Line(s)" 
        from APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY 
;

-- check

select * 
from APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY
where ASSET_PRODUCT_LINE = 'Unidentified Product'
;  
*/