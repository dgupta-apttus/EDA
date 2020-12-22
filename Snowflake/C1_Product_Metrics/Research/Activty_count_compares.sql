select count(*), PRODUCT_LINE, ACTIVITY_MONTH_DATE
FROM APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY
group by PRODUCT_LINE, ACTIVITY_MONTH_DATE
order by 3 desc, 2
;

DELETE from APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY
where ACTIVITY_MONTH_DATE = '2020-10-01'
;

select count(*), PRODUCT_LINE, ACTIVITY_DATE, SUM(ROLLING_ACTIVITY_COUNT)
FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity"
group by PRODUCT_LINE, ACTIVITY_DATE
order by 3 desc, 2
;

select count(*), PRODUCT_LINE, ACTIVITY_MONTH_DATE
FROM APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY
group by PRODUCT_LINE, ACTIVITY_MONTH_DATE
order by 3 desc, 2
;

select count(*), PRODUCT_LINE, ACTIVITY_MONTH_DATE, SUM(ACTIVITY_COUNT)
FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
group by PRODUCT_LINE, ACTIVITY_MONTH_DATE
order by 3 desc, 2
;

select count(*), PRODUCT_LINE, REPORT_DATE, SUM(ACTIVITY_COUNT)
FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY_SCORES
group by PRODUCT_LINE, REPORT_DATE
order by 3 desc, 2
;

select count(*), PRODUCT_LINE, REPORT_DATE, SUM(ACTIVITY_COUNT)
FROM APTTUS_DW.PRODUCT."Monthly_Score_Pipeline_Org"
group by PRODUCT_LINE, REPORT_DATE
order by 3 desc, 2
;

--DELETE FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY_SCORES
WHERE REPORT_DATE = '2020-10-01'
;

INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
SELECT date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_MONTHLY_ACTIVITY_SCORES' as PROC_OR_STEP