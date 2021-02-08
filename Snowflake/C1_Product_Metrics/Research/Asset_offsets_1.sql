
select distinct APTTUS_CONFIG2__PRODUCTID__C, APTTUS_CONFIG2__DESCRIPTION__C 
FROM APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY
;

with get_current_of AS (
        SELECT MAX(ACTIVITY_DATE) AS ACTIVITY_DATE, ID
        FROM   APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY
        WHERE ACTIVITY_DATE <= current_date()
        group by ID
)
, choose_current AS ( 
        SELECT 
              A.APTTUS_CONFIG2__ACCOUNTID__C      AS ACCOUNTID
            , A.ID                                AS ASSET_ID  
            , A.NAME                              AS ASSET_NAME
            , A.CREATEDDATE
            , A.CREATEDBYID
            , A.APTTUS_CONFIG2__ASSETSTATUS__C    AS ASSET_STATUS
            , A.OWNERID                           AS OWNERID
            , A.APTTUS_CONFIG2__PRODUCTID__C    
            , A.APTTUS_CONFIG2__DESCRIPTION__C    AS DESCRIPTION
            , A.PRODUCT_FAMILY__C                 AS PRODUCT_FAMILY__C     
            , A.LASTMODIFIEDBYID
            , A.LASTMODIFIEDDATE
            , A.APTTUS_CONFIG2__STARTDATE__C
            , A.APTTUS_CONFIG2__ENDDATE__C
            , A.APTTUS_CONFIG2__CANCELLEDDATE__C
            , A.CURRENCYISOCODE                   AS CURRENCY
            , COALESCE(A.ACV__C, 0)::NUMBER(19,2) AS ACV_IN_CURRENCY
            , (COALESCE(A.ACV__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS ACV
            , COALESCE(A.APTTUS_CONFIG2__NETUNITPRICE__C, 0)::NUMBER(19,2) AS NET_UNIT_PRICE_IN_CURRENCY
            , (COALESCE(A.APTTUS_CONFIG2__NETUNITPRICE__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS NET_UNIT_PRICE
            , A.APTTUS_CONFIG2__QUANTITY__C
            , COALESCE(A.APTTUS_CONFIG2__LISTPRICE__C,0)::NUMBER(19,2) AS LIST_PRICE_IN_CURRENCY
            , (COALESCE(A.APTTUS_CONFIG2__LISTPRICE__C, 0)/CT.CONVERSIONRATE)::NUMBER(19, 2) AS LIST_PRICE		         
        FROM                                     APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY A
        INNER JOIN                               get_current_of B
                                ON  A.ID = B.ID
                                AND A.ACTIVITY_DATE = B.ACTIVITY_DATE           
        LEFT OUTER JOIN                          APTTUS_DW.SF_PRODUCTION.CURRENCYTYPE  CT -- this is current only
                                ON A.CURRENCYISOCODE = CT.ISOCODE
)
/*
        SELECT * from choose_current WHERE ACCOUNTID IN ('0011U00000D8zeDQAR')
        order by accountid,  PRODUCT_FAMILY__C, APTTUS_CONFIG2__PRODUCTID__C  
        ;

        SELECT ACCOUNTID, APTTUS_CONFIG2__PRODUCTID__C, DESCRIPTION, PRODUCT_FAMILY__C, SUM(ACV_IN_CURRENCY)
        from choose_current 
        WHERE ACCOUNTID IN ('0011U00000D8w2QQAR')
        GROUP BY ACCOUNTID, APTTUS_CONFIG2__PRODUCTID__C, DESCRIPTION, PRODUCT_FAMILY__C
        ORDER by ACCOUNTID, PRODUCT_FAMILY__C, APTTUS_CONFIG2__PRODUCTID__C
      ; 
*/                               
        SELECT ACCOUNTID, --APTTUS_CONFIG2__PRODUCTID__C, 
              PRODUCT_FAMILY__C, SUM(ACV_IN_CURRENCY) as PRODUCT_SUM
        from choose_current 
        WHERE  ASSET_STATUS = 'Activated'
          AND ACCOUNTID is NOT NULL
          AND CURRENT_DATE BETWEEN APTTUS_CONFIG2__STARTDATE__C AND APTTUS_CONFIG2__ENDDATE__C
          AND PRODUCT_FAMILY__C NOT IN ('One-time Discount')
        GROUP BY ACCOUNTID, 
                 --APTTUS_CONFIG2__PRODUCTID__C, 
                 PRODUCT_FAMILY__C
        --having SUM(ACV_IN_CURRENCY) <> 0
        
;   

WITH SET_DATE_RANGE AS ( -- temporary force current
        SELECT CURRENT_DATE() AS REPORT_DATE
)          
/*WITH CONTROL_LAST AS (
        SELECT MAX(EXECUTION_TIME) AS RECENT_EXEC 
        FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
        WHERE PROC_OR_STEP = 'FILL_MONTHLY_ASSETS'
)    
, DATE_FROM_CONTROL AS (
                SELECT RUN_FOR_MONTH
                FROM APTTUS_DW.PRODUCT.PRODUCT_METRICS_RUN_CONTROL
                WHERE EXECUTION_TIME = (SELECT RECENT_EXEC FROM CONTROL_LAST)
                  AND PROC_OR_STEP = 'FILL_MONTHLY_ASSETS' 
)
, SET_DATE_RANGE AS (
        SELECT date_trunc('MONTH', (SELECT RUN_FOR_MONTH FROM DATE_FROM_CONTROL)) AS COMPLETED_MONTH
             , YEAR(COMPLETED_MONTH) AS REPORT_YEAR
             , MONTH(COMPLETED_MONTH) AS REPORT_MONTH 
             , COMPLETED_MONTH AS REPORT_DATE
)             
*/    
, get_current_of_ALI AS (
        SELECT MAX(EXTRACT_DATE) AS EXTRACT_DATE, ID
        FROM   APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY
        WHERE ACTIVITY_DATE <= (SELECT REPORT_DATE FROM SET_DATE_RANGE)
        group by ID
)
, get_current_of_C1 AS (
        SELECT MAX(EXTRACT_DATE) AS EXTRACT_DATE, ID
        FROM   APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
        WHERE ACTIVITY_DATE <= (SELECT REPORT_DATE FROM SET_DATE_RANGE)
        group by ID
)
, choose_current_C1 as (
        SELECT A.ACCOUNTID
             , 'Conga1.0'                         AS CRM
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
             , ORGID18__C                         AS CUSTOMER_ORG_SF
             , ENVIRONMENTID__C                   AS EXTERNAL_ORG
             , A.SALESFORCE_ACCOUNT__C            AS ALT_ACCOUNT --a28
             , A.SALESFORCE_ORG__C                AS ALT_ORG --a3m
             , A.ID                               AS ASSET_ID
             , A.NAME                             AS ASSET_NAME
             , A.PRODUCT2ID                       AS PRODUCT_ID 
             , B.LICENSE_TYPE__C                  AS CONFIGURATION_TYPE
             , B.NAME                             AS PRODUCT_NAME 
             , B.PRODUCT_LINE__C                  AS PRODUCT 
             , B.PRODUCTCODE
             , A.QUANTITY
             , A.CREATEDDATE
             , A.ENTITLEMENT_STATUS__C            AS ASSET_STATUS
             , A.START_DATE__C                    AS START_DATE
             , A.END_DATE__C                      AS END_DATE
             , 'USD'                              AS CURRENCY
             , COALESCE((A.MRR_ASSET_MRR__C * 12),0) AS ACV_IN_CURRENCY
             , COALESCE((A.MRR_ASSET_MRR__C * 12),0) AS ACV
             , COALESCE(A.PRICE, 0)               AS NET_UNIT_PRICE_IN_CURRENCY 
             , COALESCE(A.PRICE, 0)               AS NET_UNIT_PRICE            
        FROM                     APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY A
        INNER JOIN               get_current_of_C1 D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE           
        LEFT OUTER JOIN          APTTUS_DW.SF_CONGA1_1.PRODUCT2 B -- should this be a hist snapshot?
                            ON A.PRODUCT2ID = B.ID
        WHERE A.ENTITLEMENT_STATUS__C = 'Active'    
          AND A.TYPE__C = 'Subscription'                 
)
, choose_current_ALI AS ( 
                select A.APTTUS_CONFIG2__ACCOUNTID__C      AS ACCOUNTID
                     , 'Apttus1.0'                         AS CRM  
                     , 'SALESFORCE'                        AS ORG_SOURCE    
                     , A.APTTUS_CONFIG2__SHIPTOACCOUNTID__C AS ALT_ACCOUNT
                     , A.ID                                AS ASSET_ID  
                     , A.NAME                              AS ASSET_NAME
                     , A.APTTUS_CONFIG2__PRODUCTID__C      AS PRODUCT_ID
                     , B.APTTUS_CONFIG2__CONFIGURATIONTYPE__C AS CONFIGURATION_TYPE
                     , B.NAME                              AS PRODUCT_NAME
                --     , A.APTTUS_CONFIG2__DESCRIPTION__C    AS PRODUCT_DESCRIPTION_FROM_ASSET
                     , B.FAMILY                            AS PRODUCT
                --     , A.PRODUCT_FAMILY__C                 AS FAMILY_FROM_ASSET 
                     , B.NS_PRODUCT_ID__C
                     , B.PRODUCTCODE      
                     , A.APTTUS_CONFIG2__QUANTITY__C       AS QUANTITY    
                     , A.CREATEDDATE
                     , A.APTTUS_CONFIG2__ASSETSTATUS__C    AS ASSET_STATUS
                     , A.APTTUS_CONFIG2__STARTDATE__C      AS START_DATE
                     , A.APTTUS_CONFIG2__ENDDATE__C        AS END_DATE
                     , A.APTTUS_CONFIG2__CANCELLEDDATE__C  AS CANCELLED_DATE
                     , A.CURRENCYISOCODE                   AS CURRENCY
                     , COALESCE(A.ACV__C, 0)::NUMBER(19,2) AS ACV_IN_CURRENCY
                     , (COALESCE(A.ACV__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS ACV
                     , A.APTTUS_CONFIG2__ASSETTCV__C
                     , COALESCE(A.APTTUS_CONFIG2__NETUNITPRICE__C, 0)::NUMBER(19,2) AS NET_UNIT_PRICE_IN_CURRENCY
                     , (COALESCE(A.APTTUS_CONFIG2__NETUNITPRICE__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS NET_UNIT_PRICE
                     , COALESCE(A.APTTUS_CONFIG2__LISTPRICE__C,0)::NUMBER(19,2) AS LIST_PRICE_IN_CURRENCY
                     , (COALESCE(A.APTTUS_CONFIG2__LISTPRICE__C, 0)/CT.CONVERSIONRATE)::NUMBER(19, 2) AS LIST_PRICE     
        FROM                     APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY A
        INNER JOIN               get_current_of_ALI D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE          
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.PRODUCT2 B -- should this be a hist snapshot?
                            ON A.APTTUS_CONFIG2__PRODUCTID__C = B.ID
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.CURRENCYTYPE  CT -- this is current only
                            ON A.CURRENCYISOCODE = CT.ISOCODE                    
)

select ACCOUNTID, CRM, ORG_SOURCE, CUSTOMER_ORG, PRODUCT, PRODUCT_ID, SUM(QUANTITY), SUM(ACV)
FROM  choose_current_C1 
GROUP BY ACCOUNTID, CRM, ORG_SOURCE, CUSTOMER_ORG, PRODUCT, PRODUCT_ID
;

        SELECT A.APTTUS_CONFIG2__ACCOUNTID__C      AS ACCOUNTID
             , 'Apttus1.0'                         AS CRM  
             , A.APTTUS_CONFIG2__PRODUCTID__C      AS PRODUCT_ID
             , COALESCE(B.FAMILY, 'Unidentified Product') AS PRODUCT
             , 'SALESFORCE'                        AS ORG_SOURCE    
             , 'Unknown'                           AS CUSTOMER_ORG               
             , A.APTTUS_CONFIG2__QUANTITY__C       AS QUANTITY    
             , (COALESCE(A.ACV__C, 0)/CT.CONVERSIONRATE)::NUMBER(19,2) AS ACV
             , SUM(ACV) OVER (PARTITION BY A.ACCOUNTID) AS ACV_ON_ACCOUN   
        FROM                     APTTUS_DW.SNAPSHOTS.ASSETLINEITEM_HISTORY A
        INNER JOIN               get_current_of_ALI D
                            ON  A.ID = D.ID
                            AND A.EXTRACT_DATE = D.EXTRACT_DATE          
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.PRODUCT2 B -- should this be a hist snapshot?
                            ON A.APTTUS_CONFIG2__PRODUCTID__C = B.ID
        LEFT OUTER JOIN          APTTUS_DW.SF_PRODUCTION.CURRENCYTYPE  CT -- this is current only
                            ON A.CURRENCYISOCODE = CT.ISOCODE   
        WHERE A1_STATUS = 'Activated'
          AND (SELECT REPORT_DATE FROM SET_DATE_RANGE) BETWEEN START_DATE AND END_DATE 


