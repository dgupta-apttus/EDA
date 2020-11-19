--INSERT INTO APTTUS_DW.PRODUCT.LMA_LICENSE_MONTHLY 
--()
with step1 as ( 
        SELECT 
        FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY
        where Year(ACTIVITY_DATE) = YEAR(dateadd(month, -1, CURRENT_DATE()))
          and Month(ACTIVITY_DATE) = MONTH(dateadd(month, -1, CURRENT_DATE()))
        group by 
)
        SELECT              
        FROM                   step1 A
        INNER JOIN             APTTUS_DW.SF_PRODUCTION."DateDim" B
                     ON  A.ACTIVITY_YEAR = B."Calendar_Year"
                     AND A.ACTIVITY_MONTH = B."Calendar_Month"
                     AND B."Day" = 1 
                     
;

