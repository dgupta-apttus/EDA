--INSERT INTO APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_PERC_HISTORY    
WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_MONTHLY_ASSETS'
)    
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_MONTHLY_ASSETS' 
                  AND STATUS <> 'Complete'
)
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL)) AS COMPLETED_MONTH
             , YEAR(COMPLETED_MONTH) AS REPORT_YEAR
             , MONTH(COMPLETED_MONTH) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE -- will report on beginning of month
             , date_trunc('MONTH',dateadd(month, 1, COMPLETED_MONTH)) AS LESS_THAN_DATE -- values will represent state at end of month
)   
, product_by_asset_name as (
        SELECT  ASSET_NAME
             , listagg(distinct C2_PRODUCTFAMILY, ', ') AS PRODUCT
        FROM APTTUS_DW.SF_PRODUCTION.C2_ASSETMAPPING_V1	
        group by ASSET_NAME
)          
, get_current_of_C1 AS (
        SELECT MAX(EXTRACT_DATE) AS EXTRACT_DATE, ID
        FROM   APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
        WHERE ACTIVITY_DATE <= (SELECT LESS_THAN_DATE FROM SET_DATE_RANGE)
        group by ID
)
, get_current_of_ALI AS (
        SELECT MAX(EXTRACT_DATE) AS EXTRACT_DATE, ID
        FROM   APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY
        WHERE ACTIVITY_DATE <= (SELECT LESS_THAN_DATE FROM SET_DATE_RANGE)
        group by ID
)
, choose_current_asset as (
        SELECT A.ACCOUNTID
             , 'Conga1.0'                         AS CRM
             , A.PRODUCT2ID                       AS PRODUCT_ID 
             , COALESCE(B.PRODUCT_LINE__C, 'Unidentified Product') AS PRODUCT_PRODUCT
             , COALESCE(E.PRODUCT,  'Unidentified Product') AS ASSET_PRODUCT 
             , CASE
                 WHEN B.PRODUCT_LINE__C = 'Conga Contracts'
                   THEN 'CONTRACTS'
                 WHEN B.PRODUCT_LINE__C = 'Conga Collaborate'
                   THEN 'COLLABORATE'
                ELSE 'SALESFORCE'
               END                                AS ORG_SOURCE                                
             , CASE
                 WHEN (ENVIRONMENTID__C is NULL
                   OR ENVIRONMENTID__C LIKE '00D%')
                     THEN COALESCE(ORGID18__C, 'Unknown')
                 WHEN B.PRODUCT_LINE__C IN ('Conga Collaborate', 'Conga Contracts')
                     THEN ENVIRONMENTID__C
                ELSE COALESCE(ORGID18__C, 'Unknown')     
               END                                AS CUSTOMER_ORG 
             , A.QUANTITY
             , COALESCE((A.MRR_ASSET_MRR__C * 12),0) AS ACV   
             , SUM(ACV) OVER (PARTITION BY A.ACCOUNTID) AS ACV_ON_ACCOUNT       
        FROM                     APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY A
        INNER JOIN               get_current_of_C1 D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE           
        LEFT OUTER JOIN          APTTUS_DW.SF_CONGA1_1.PRODUCT2 B -- should this be a hist snapshot?
                            ON A.PRODUCT2ID = B.ID
        LEFT OUTER JOIN          product_by_asset_name E
                            ON A.NAME = E.ASSET_NAME          
        WHERE A.ENTITLEMENT_STATUS__C = 'Active'    
          AND A.TYPE__C = 'Subscription'                 
UNION --
        SELECT A.APTTUS_CONFIG2__ACCOUNTID__C      AS ACCOUNTID
             , 'Apttus1.0'                         AS CRM  
             , A.APTTUS_CONFIG2__PRODUCTID__C      AS PRODUCT_ID
             , COALESCE(B.PRODUCT_LINE__C, 'Unidentified Product') AS PRODUCT_PRODUCT
             , COALESCE(E.PRODUCT,  'Unidentified Product') AS ASSET_PRODUCT 
             , 'SALESFORCE'                        AS ORG_SOURCE    
             , 'Unknown'                           AS CUSTOMER_ORG               
             , A.APTTUS_CONFIG2__QUANTITY__C       AS QUANTITY    
             , (COALESCE(A.ACV__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS ACV
             , SUM(ACV) OVER (PARTITION BY A.APTTUS_CONFIG2__ACCOUNTID__C) AS ACV_ON_ACCOUNT   
        FROM                     APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY A
        INNER JOIN               get_current_of_ALI D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE          
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.PRODUCT2 B -- should this be a hist snapshot?
                            ON A.APTTUS_CONFIG2__PRODUCTID__C = B.ID
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.CURRENCYTYPE  CT -- this is current only
                            ON A.CURRENCYISOCODE = CT.ISOCODE
        LEFT OUTER JOIN          product_by_asset_name E
                            ON A.NAME = E.ASSET_NAME                                
        WHERE A.APTTUS_CONFIG2__ASSETSTATUS__C = 'Activated'
          AND (SELECT LESS_THAN_DATE FROM SET_DATE_RANGE) BETWEEN A.APTTUS_CONFIG2__STARTDATE__C AND A.APTTUS_CONFIG2__ENDDATE__C 
) 
SELECt *        FROM  choose_current_asset
;           
, sum_product_c1_pos as (
        select ACCOUNTID
             , CRM
             , ASSET_PRODUCT
             , SUM(ACV) AS ACV
        FROM  choose_current_asset 
        GROUP BY ACCOUNTID
               , CRM
               , PRODUCT
        HAVING SUM(ACV) > 0
           AND MAX(ACV_ON_ACCOUNT) > 0  
)
, sum_product_c1_all as (
        select ACCOUNTID
             , CRM
             , PRODUCT
             , SUM(QUANTITY) as QUANTITY
             , COUNT(distinct CUSTOMER_ORG) as ORG_COUNT
             , COUNT(distinct PRODUCT_ID) as PRODUCT_ID_COUNT 
             , SUM(ACV) AS ACV
             , MAX(ACV_ON_ACCOUNT) as ACV_ON_ACCOUNT                 
        FROM  choose_current_asset 
        GROUP BY ACCOUNTID
               , CRM
               , PRODUCT
)
--, inner1 as (
        SELECT A.ACCOUNTID
             , A.CRM
             , A.PRODUCT
             , (SELECT REPORT_DATE FROM SET_DATE_RANGE) AS REPORT_DATE
             , A.QUANTITY
             , A.ORG_COUNT
             , A.PRODUCT_ID_COUNT
             , ROUND(A.ACV, 2) as PRODUCT_ACV
             , ROUND(A.ACV_ON_ACCOUNT, 2) AS ACV_ON_ACCOUNT
             , CASE 
                 WHEN B.ACV is not null 
                   THEN ROUND((B.ACV*100)/(SUM(B.ACV) OVER (PARTITION BY B.ACCOUNTID)), 1)
                ELSE 0
               END AS PRODUCT_ACV_PERCENTAGE
             , COALESCE(ROUND(SUM(B.ACV) OVER (PARTITION BY B.ACCOUNTID), 2), 0) AS POSITIVE_ACCOUNT_ACV  -- this is for precentage calcs only here for check you work
        FROM                    sum_product_c1_all A
        LEFT OUTER JOIN         sum_product_c1_pos B
                          ON  A.ACCOUNTID = B.ACCOUNTID
                          AND A.CRM = B.CRM
                          AND A.PRODUCT = B.PRODUCT             
--order by 1, 3        
--
;                 
                   
/*
, sum_org_c1 as (
        select ACCOUNTID
             , CRM
             , PRODUCT
             , PRODUCT_ID
             , ORG_SOURCE
             , CUSTOMER_ORG
             , SUM(QUANTITY) as QUANTITY
             , SUM(ACV) AS ACV
             , MAX(ACV_ON_ACCOUNT) as ACV_ON_ACCOUNT
        FROM  choose_current_C1 
        GROUP BY ACCOUNTID
               , CRM
               , PRODUCT
               , PRODUCT_ID
               , ORG_SOURCE
               , CUSTOMER_ORG               
        HAVING SUM(ACV) > 0
           AND MAX(ACV_ON_ACCOUNT) > 0      
;)
, sum_PID_c1 as (
        select ACCOUNTID
             , CRM
             , PRODUCT
             , PRODUCT_ID
             , SUM(QUANTITY) as QUANTITY
             , SUM(ACV) AS ACV
             , MAX(ACV_ON_ACCOUNT) as ACV_ON_ACCOUNT
        FROM  choose_current_C1 
        GROUP BY ACCOUNTID
               , CRM
               , PRODUCT
               , PRODUCT_ID
        HAVING SUM(ACV) > 0
           AND MAX(ACV_ON_ACCOUNT) > 0      
)
*/    

--INSERT INTO APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL (RUN_FOR_MONTH, OPERATOR, EXECUTION_TIME, STATUS, PROC_OR_STEP)
--SELECT date_trunc('MONTH',dateadd(month, -1, CURRENT_DATE())) AS RUN_FOR_MONTH -- USE THIS TO MANUALLY OVERRIDE THE RUN DATE OTHERWISE IT WILL DEFAULT
     , CURRENT_USER() AS OPERATOR
     , CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP()) AS EXECUTION_TIME
     , 'Incomplete' AS STATUS
     , 'FILL_MONTHLY_ASSETS' as PROC_OR_STEP
;
--WITH SET_DATE_RANGE AS ( -- temporary force current
--      SELECT CURRENT_DATE() AS REPORT_DATE
--        
--)     

--delete from APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_PERC_HISTORY;
               
                   