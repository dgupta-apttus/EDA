

--INSERT INTO APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY   
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
             , 'Conga1.0'                                                   AS CRM
             , A.NAME                                                       AS ASSET_NAME
             , (SELECT REPORT_DATE FROM SET_DATE_RANGE)                     AS REPORT_DATE
             , A.PRODUCT2ID                                                 AS PRODUCT_ID 
             , B.NAME                                                       AS PRODUCT_NAME
             , COALESCE(B.PRODUCT_LINE__C, 'Unidentified Product')          AS PRODUCT_LINE
             , COALESCE(E.PRODUCT,  'Unidentified Product')                 AS ASSET_PRODUCT_LINE 
             , CASE
                 WHEN B.PRODUCT_LINE__C = 'Conga Contracts'
                   THEN 'CONTRACTS'
                 WHEN B.PRODUCT_LINE__C = 'Conga Collaborate'
                   THEN 'COLLABORATE'
                ELSE 'SALESFORCE'
               END                                                          AS ORG_SOURCE                                
             , CASE
                 WHEN (ENVIRONMENTID__C is NULL
                   OR ENVIRONMENTID__C LIKE '00D%')
                     THEN COALESCE(ORGID18__C, 'Unknown')
                 WHEN B.PRODUCT_LINE__C IN ('Conga Collaborate', 'Conga Contracts')
                     THEN ENVIRONMENTID__C
                ELSE COALESCE(ORGID18__C, 'Unknown')     
               END                                                          AS CUSTOMER_ORG 
             , A.QUANTITY
             , COALESCE((A.MRR_ASSET_MRR__C * 12),0)                        AS ACV   
             , SUM(ACV) OVER (PARTITION BY A.ACCOUNTID)                     AS ACV_ON_ACCOUNT     
             , 'Active'                                                     AS ASSET_STATUS  
             , A.START_DATE__C                                              AS START_DATE 
             , A.END_DATE__C                                                AS END_DATE
             , 'USD'                                                        AS ORIGINAL_CURRENCY
             , ACV                                                          AS ORIGINAL_ACV             
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
        SELECT A.APTTUS_CONFIG2__ACCOUNTID__C                               AS ACCOUNTID
             , 'Apttus1.0'                                                  AS CRM  
             , A.NAME                                                       AS ASSET_NAME
             , (SELECT REPORT_DATE FROM SET_DATE_RANGE)                     AS REPORT_DATE
             , A.APTTUS_CONFIG2__PRODUCTID__C                               AS PRODUCT_ID
             , B.NAME                                                       AS PRODUCT_NAME
             , COALESCE(B.FAMILY, 'Unidentified Product')                   AS PRODUCT_LINE
             , COALESCE(E.PRODUCT,  'Unidentified Product')                 AS ASSET_PRODUCT_LINE 
             , 'SALESFORCE'                                                 AS ORG_SOURCE    
             , 'Unknown'                                                    AS CUSTOMER_ORG               
             , A.APTTUS_CONFIG2__QUANTITY__C                                AS QUANTITY    
             , (COALESCE(A.ACV__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2)      AS ACV
             , SUM(ACV) OVER (PARTITION BY A.APTTUS_CONFIG2__ACCOUNTID__C)  AS ACV_ON_ACCOUNT   
             , 'Active'                                                     AS ASSET_STATUS
             , A.APTTUS_CONFIG2__STARTDATE__C                               AS START_DATE 
             , A.APTTUS_CONFIG2__ENDDATE__C                                 AS END_DATE
             , A.CURRENCYISOCODE                                            AS ORIGINAL_CURRENCY
             , A.ACV__C                                                     AS ORIGINAL_ACV
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
select * 
from choose_current_asset 
;

/*
select ACCOUNTID                    AS "Account ID"
      , CRM                         AS "CRM" 
      , ASSET_NAME                  AS "Asset Name"
      , REPORT_DATE                 AS "Report Date"
      , PRODUCT_ID                  AS "Product ID"
      , PRODUCT_LINE                AS "Product Line"  
      , ASSET_PRODUCT_LINE          AS "Asset Product Line" 
      , ORG_SOURCE                  AS "Org Source"
      , CUSTOMER_ORG                AS "Customer Org" 
      , QUANTITY                    AS "Quantity"
      , ACV                         AS "ACV"
      , ACV_ON_ACCOUNT              AS "ACV on Account" 
      , ASSET_STATUS                AS "Asset Status"
      , START_DATE                  AS "Asset Start Date"
      , END_DATE                    AS "Asset End Date" 
      , ORIGINAL_CURRENCY           AS "Currency"  
      , ORIGINAL_ACV                AS "Local ACV"
FROM APTTUS_DW.PRODUCT.ACCOUNT_ASSET_PRODUCT_HISTORY
;
*/