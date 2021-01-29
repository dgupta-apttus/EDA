

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
/*        SELECT A.ACCOUNTID
             , 'Conga1.0'                                                   AS CRM
             , A.ID                                                         AS ASSET_ID             
             , A.NAME                                                       AS ASSET_NAME
             , (SELECT REPORT_DATE FROM SET_DATE_RANGE)                     AS REPORT_DATE
             , A.PRODUCT2ID                                                 AS PRODUCT_ID 
             , B.PRODUCT_NAME
             , COALESCE(B.PRODUCT, 'Unidentified Product')                  AS PRODUCT
             , B.PRODUCT_LINE
             , B.PRODUCT_FAMILY
             , B.PRODUCT2_LINE 
             , CASE
                 WHEN B.PRODUCT = 'Conga Contracts'
                   THEN 'CONTRACTS'
                 WHEN B.PRODUCT = 'Conga Collaborate'
                   THEN 'COLLABORATE'
                ELSE 'SALESFORCE'
               END                                                          AS ORG_SOURCE                                
             , CASE
                 WHEN (ENVIRONMENTID__C is NULL
                   OR ENVIRONMENTID__C LIKE '00D%')
                     THEN COALESCE(ORGID18__C, 'Unknown')
                 WHEN B.PRODUCT IN ('Conga Collaborate', 'Conga Contracts')
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
        LEFT OUTER JOIN         APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_ASSET_MAPPING B
                            ON A.PRODUCT2ID = B.PRODUCT_ID        
        WHERE A.ENTITLEMENT_STATUS__C = 'Active'    
          AND A.TYPE__C = 'Subscription'                 
UNION -- */
        SELECT A.APTTUS_CONFIG2__ACCOUNTID__C                               AS ACCOUNTID
             , 'Apttus1.0'                                                  AS CRM  
             , A.ID                                                         AS ASSET_ID
             , A.NAME                                                       AS ASSET_NAME
             , (SELECT REPORT_DATE FROM SET_DATE_RANGE)                     AS REPORT_DATE
             , A.APTTUS_CONFIG2__PRODUCTID__C                               AS PRODUCT_ID
             , B.PRODUCT_NAME
             , COALESCE(B.PRODUCT, 'Unidentified Product')                  AS PRODUCT
             , B.PRODUCT_LINE
             , B.PRODUCT_FAMILY
             , B.PRODUCT2_LINE 
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
             
             , A.APTTUS_CONFIG2__ASSETSTATUS__C             
        FROM                     APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY A
        INNER JOIN               get_current_of_ALI D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE          
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.CURRENCYTYPE  CT -- this is current only
                            ON A.CURRENCYISOCODE = CT.ISOCODE
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_ASSET_MAPPING B
                            ON A.APTTUS_CONFIG2__PRODUCTID__C = B.PRODUCT_ID                                   
        WHERE A.APTTUS_CONFIG2__ASSETSTATUS__C = 'Activated'
          AND (SELECT LESS_THAN_DATE FROM SET_DATE_RANGE) BETWEEN A.APTTUS_CONFIG2__STARTDATE__C AND A.APTTUS_CONFIG2__ENDDATE__C 
)
select * 
from choose_current_asset
                               where ACCOUNTID = '0011U00000D92bqQAB'
;


select ACCOUNT_NAME, ACCOUNTID_18__C
from APTTUS_DW.SF_PRODUCTION."Account_C2"
where UPPER(ACCOUNT_NAME) LIKE 'REED BUS%'
;

select *
from APTTUS_DW.SF_PRODUCTION."Asset_Account_C2"
where ACCOUNTID_18__C = '0011U00000D92bqQAB'
;

WITH FILTER_ASSETS  AS (
SELECT
	A."SOURCE",
	A.ACCOUNTID,
	A.ACCOUNT_NAME,
	A.ASSET_NAME,
	A.C1_STATUS,
	A.A1_STATUS,
	CASE 
	    WHEN C1_STATUS = 'Active'
	        THEN 'Active'
	    WHEN  A1_STATUS = 'Activated'
	        THEN 'Active'
	END  AS STATUS,  
   	A.PRODUCT_FAMILY__C,
	A.OWNERID,
    A.ACV_IN_CURRENCY,
	A.ACV,
    SUM(A.ACV) OVER (PARTITION BY A.ACCOUNTID) AS ACV_ON_ACCOUNT,
	A.DESCRIPTION,
    A.DELETE_FLAG,
	A.RECORD_TYPE,
	A.START_DATE,
	A.END_DATE,
    A.PRICE_IN_CURRENCY,
	A.PRICE,
	A.QUANTITY,
	A.ACCOUNTING_STAGE,
	A.CREATEDDATE,
	A.CREATEDBYID,
	A.LASTMODIFIEDBYID,
	A.LASTMODIFIEDDATE,
	A.OPPTY_RECORD_TYPE,
	A.SALES_OPS_APPROVED,
	A.PRODUCT_NAME,
	A.PRODUCT_LINE__C,
    A.LIST_PRICE_IN_CURRENCY,
    A.LIST_PRICE
FROM APTTUS_DW.SF_PRODUCTION."Asset_C2" A
--WHERE (C1_STATUS = 'Active')
--OR    (A1_STATUS = 'Activated'
--      AND 
WHERE      CURRENT_DATE() BETWEEN START_DATE AND END_DATE 
--      )
)
select *
from FILTER_ASSETS
where ACCOUNTID = '0011U00000D92bqQAB'
;

with innerA1 as (
SELECT 
      A.APTTUS_CONFIG2__ACCOUNTID__C      AS ACCOUNTID
    , A.NAME                              AS ASSET_NAME
    , NULL                                AS ACCOUNTING_STAGE
    , A.CREATEDDATE
    , A.CREATEDBYID
    , A.DELETE_FLAG__C                    AS DELETE_FLAG
    , A.APTTUS_CONFIG2__ASSETSTATUS__C    AS ASSET_STATUS
    , A.PRODUCT_FAMILY__C                 AS PRODUCT_FAMILY__C 
    , A.OWNERID                           AS OWNERID
    , A.APTTUS_CONFIG2__DESCRIPTION__C    AS DESCRIPTION
    , A.LASTMODIFIEDBYID
    , A.LASTMODIFIEDDATE
    , NULL                               AS RECORD_TYPE
    , A.APTTUS_CONFIG2__STARTDATE__C
    , A.APTTUS_CONFIG2__ENDDATE__C
    , A.CURRENCYISOCODE                   AS CURRENCY
    , COALESCE(A.ACV__C, 0)::NUMBER(19,2) AS ACV_IN_CURRENCY
    , (COALESCE(A.ACV__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS ACV
    , COALESCE(A.APTTUS_CONFIG2__NETUNITPRICE__C, 0)::NUMBER(19,2) AS NET_UNIT_PRICE_IN_CURRENCY
    , (COALESCE(A.APTTUS_CONFIG2__NETUNITPRICE__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS NET_UNIT_PRICE
    , A.APTTUS_CONFIG2__QUANTITY__C
    , COALESCE(A.APTTUS_CONFIG2__LISTPRICE__C,0)::NUMBER(19,2) AS LIST_PRICE_IN_CURRENCY
    , (COALESCE(A.APTTUS_CONFIG2__LISTPRICE__C, 0)/CT.CONVERSIONRATE)::NUMBER(19, 2) AS LIST_PRICE
    , B.ACCOUNT_NAME			         

FROM                                     APTTUS_DW.SF_PRODUCTION.APTTUS_CONFIG2__ASSETLINEITEM__C A
LEFT OUTER JOIN                          APTTUS_DW.SF_PRODUCTION."Account_A1" B
                                     ON  A.APTTUS_CONFIG2__ACCOUNTID__C = B.ACCOUNTID_18
LEFT OUTER JOIN APTTUS_DW.SF_PRODUCTION.CURRENCYTYPE  CT
                                     ON A.CURRENCYISOCODE = CT.ISOCODE
)
select *
from  innerA1
where ACCOUNTID = '0011U00000D92bqQAB'
                                     
;
select * 
FROM APTTUS_DW.SF_PRODUCTION.APTTUS_CONFIG2__ASSETLINEITEM__C
WHERE APTTUS_CONFIG2__ACCOUNTID__C = '0011U00000D92bqQAB'
;

select ID, SNAP_LOAD_AT, *
from APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY
WHERE APTTUS_CONFIG2__ACCOUNTID__C = '0011U00000D92bqQAB'
order by 1, 2
;


--select count(*) 
--from choose_current_asset
/*

select count(*), ASSET_ID
from choose_current_asset
group by ASSET_ID
having count(*) > 1

select ACCOUNTID                    AS "Account ID"
      , CRM                         AS "CRM" 
      , ASSET_NAME                  AS "Asset Name"
      , REPORT_DATE                 AS "Report Date"
      , PRODUCT_ID                  AS "Product ID"
      , PRODUCT_NAME                AS "Product Name"
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