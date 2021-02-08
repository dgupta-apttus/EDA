CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.LICENSE_MONTHLY_DATES
COMMENT = 'Month by month veiw of license and user tokens
-- 2021/01/12 adapted from License_Monthly_History - gdw
'
AS   
SELECT * from APTTUS_DW.SF_PRODUCTION."Dates"
WHERE "Date" in (
        select distinct "Report Month Date" as "Report Month Date"
        from APTTUS_DW.PRODUCT."License_Monthly_History"
)         
;