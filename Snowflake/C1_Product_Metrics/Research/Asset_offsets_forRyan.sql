
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
        SELECT ACCOUNTID, APTTUS_CONFIG2__PRODUCTID__C, PRODUCT_FAMILY__C, SUM(ACV_IN_CURRENCY) as PRODUCT_SUM
        from choose_current 
        WHERE  ASSET_STATUS = 'Activated'
          AND ACCOUNTID is NOT NULL
          AND CURRENT_DATE BETWEEN APTTUS_CONFIG2__STARTDATE__C AND APTTUS_CONFIG2__ENDDATE__C
        GROUP BY ACCOUNTID, APTTUS_CONFIG2__PRODUCTID__C, PRODUCT_FAMILY__C
        having SUM(ACV_IN_CURRENCY) <> 0
        
;                                     