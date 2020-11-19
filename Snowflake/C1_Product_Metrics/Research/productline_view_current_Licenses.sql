

select LICENSE_ID
        , CUSTOMER_ORG_18
        , CUSTOMER_ORG_15  
        , PACKAGE_NAME
        , PACKAGE_ID
        , PACKAGE_VERSION_ID
        , ORG_PACKAGE
        , PRODUCT_LINE
        , STATUS
        , ORG_STATUS 
        , ACCOUNT_ID
        , ACCOUNT_NAME  
        , IS_SANDBOX
        , PREDICTED_PACKAGE_NAMESPACE 
        , LICENSE_SEAT_TYPE    
        , SEATS 
        , USED_LICENSES        
        , INSTALL_DATE
        , UNINSTALL_DATE
        , MONTHS_INSTALLED
        , INSTALL_DATE_STRING  
        , EXPIRATION_DATE
        , EXPIRATION_DATE_STRING                           
        , PACKAGE_SORT
        , STATUS_SORT               
        , SELECT1_FOR_PRODUCT_LINE        
        , SELECT1_FOR_PACKAGE_ID
        , LAST_ACTIVITY_DATE
from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
;

INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY 
WITH LISTS AS (
        select    listagg(DISTINCT PACKAGE_NAME, ', ') within group (ORDER BY PACKAGE_SORT) AS PACKAGE_LIST 
                , listagg(DISTINCT PACKAGE_ID, ', ') within group (ORDER BY PACKAGE_ID) AS PACKAGE_ID_LIST
                , listagg(DISTINCT LICENSE_ID, ', ') within group (ORDER BY LICENSE_ID) AS LICENSE_ID_LIST
                , CUSTOMER_ORG_18
                , PRODUCT_LINE
                , COUNT(*) AS LICENSE_COUNT
                , COUNT(DISTINCT PACKAGE_ID) AS PACKAGE_COUNT
                , MAX(MONTHS_INSTALLED) AS LONGEST_INSTALL 
        from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
        WHERE UPPER(STATUS) = 'ACTIVE'
        group by CUSTOMER_ORG_18, PRODUCT_LINE
) 
select  
          A.CUSTOMER_ORG_18
        , A.PRODUCT_LINE
        , '2020-09-01' AS REPORTING_MONTH -- set to previous month
        , A.LICENSE_ID AS PRIMARY_LICENSE_ID
        , COALESCE(B.LICENSE_ID_LIST, 'None Active') AS LICENSE_ID_LIST
        , COALESCE(B.LICENSE_COUNT, 0) AS LICENSE_COUNT
        , A.CUSTOMER_ORG_15  
        , A.PACKAGE_NAME
        , COALESCE(B.PACKAGE_LIST, 'None Active') AS PACKAGE_LIST
        , A.PACKAGE_ID
        , COALESCE(B.PACKAGE_ID_LIST, 'None Active') AS PACKAGE_ID_LIST
        , COALESCE(B.PACKAGE_COUNT, 0) AS PACKAGE_COUNT
        , PACKAGE_VERSION_ID
        , ORG_PACKAGE
        , STATUS
        , ORG_STATUS 
        , ACCOUNT_ID
        , ACCOUNT_NAME  
        , IS_SANDBOX
        , PREDICTED_PACKAGE_NAMESPACE 
        , LICENSE_SEAT_TYPE    
        , SEATS 
        , USED_LICENSES        
        , INSTALL_DATE
        , UNINSTALL_DATE
        , MONTHS_INSTALLED
        , INSTALL_DATE_STRING
        , COALESCE(B.LONGEST_INSTALL, MONTHS_INSTALLED) AS LONGEST_INSTALL          
        , EXPIRATION_DATE
        , EXPIRATION_DATE_STRING                           
        , LAST_ACTIVITY_DATE
        , SUSPEND_ACCOUNT_BOOL
        , ACCOUNT_SUSPENDED_REASON
        , LICENSE_NAME
        , C1_PRODUCTION_BOOL
        , 'C1_Snapshots' AS DATA_SOURCE
FROM                          APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT A
LEFT OUTER JOIN               LISTS B
              ON  A.CUSTOMER_ORG_18 = B.CUSTOMER_ORG_18
              AND A.PRODUCT_LINE = B.PRODUCT_LINE
WHERE A.SELECT1_FOR_PRODUCT_LINE = 1
 AND (   LAST_ACTIVITY_DATE >= (CURRENT_DATE()-75)
      OR (     LAST_ACTIVITY_DATE < (CURRENT_DATE()-75)
          AND  UPPER(STATUS) = 'ACTIVE'
          AND  EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED', 'EXPIRED') 
         ) 
     )    
;     
        
)
select count(*) from inner1
--select * from inner1
WHERE STATUS <> 'Active'  and LAST_ACTIVITY_DATE < (CURRENT_DATE()-75)       
;


select count(*) from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
WHERE SELECT1_FOR_PRODUCT_LINE = 1
 AND (   LAST_ACTIVITY_DATE >= (CURRENT_DATE()-75)
      OR (     LAST_ACTIVITY_DATE < (CURRENT_DATE()-75)
          AND  UPPER(STATUS) = 'ACTIVE'
          AND  EXPIRATION_DATE_STRING NOT IN ('UNINSTALLED', 'EXPIRED') 
         ) 
     ) 
--WHERE NOT (    UPPER(STATUS) <> 'ACTIVE' 
--           and LAST_ACTIVITY_DATE > (CURRENT_DATE()-90)
--          ) 
;

select distinct EXPIRATION_DATE_STRING
from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
;

select * from real1 order by LAST_ACTIVITY_DATE desc;
--select distinct PACKAGE_NAME, PRODUCT_LINE, PACKAGE_ID from real1;
--select distinct STATUS from real1;
--select min(LAST_ACTIVITY_DATE) from real1;

select * from real1
WHERE CUSTOMER_ORG_18 IN
    (SELECT distinct CUSTOMER_ORG_18 from real1 where SELECT1_FOR_PACKAGE_ID > 1)     
order by  CUSTOMER_ORG_18, PRODUCT_LINE, PACKAGE_ID
;                 

SELECT *
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY
WHERE ID = 'a0250000005DoYMAA0' 
;                 
--WHERE --L.CUSTOMER_ORG_ID__C = ''
-- test
  --and 
--   ID = 'a021T00000yTFS8QAO'
  --    SFLMA__ORG_STATUS__C  is null
  ;
select * FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT   
WHERE ID in (
select ID from APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT 
where --SFLMA__ORG_STATUS__C  is null or 
      SFLMA__ORG_STATUS__C = ''
  and SFLMA__LICENSE_STATUS__C = 'Active'
)
; 


--LMA_LIC_PRODUCTLINE_CURRENT 
/*
SELECT    L.ID as LICENSE_ID
        , L.CUSTOMER_ORG_ID__C                      AS CUSTOMER_ORG_18
        , L.SFLMA__SUBSCRIBER_ORG_ID__C             AS CUSTOMER_ORG_15  
        , L.PACKAGE_NAMEFX__C                       AS PACKAGE_NAME
        , L.SFLMA__PACKAGE__C                       AS PACKAGE_ID
        , L.SFLMA__PACKAGE_VERSION__C               AS PACKAGE_VERSION_ID
        , CUSTOMER_ORG_15 || '-' || PACKAGE_ID      AS ORG_PACKAGE
        , P.PRODUCT_LINE
--        , (SELECT END_DATE::TIMESTAMP FROM LOAD_1_ADOPTION_PARAMS) AS RECORD_TIMESTAMP  
        , L.SFLMA__LICENSE_STATUS__C                AS STATUS
        , COALESCE(SFLMA__ORG_STATUS__C, 'Unknown') AS ORG_STATUS 
        , L.SFLMA__ACCOUNT__C                       AS ACCOUNT_ID
        , null                                      AS ACCOUNT_NAME  
        , L.SFLMA__IS_SANDBOX__C                    AS IS_SANDBOX
        , CASE
                 WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA COMPOSER'
                   THEN 'APXTCONGA4'
                 WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'SALESFORCE CPQ: CONGA QUOTES'
                   THEN 'APXTCFQ'   
                 WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA INVOICE GENERATION'  
                   THEN 'UNKNOWN COMPOSER'
                 ELSE 'OTHER'  
          END AS PREDICTED_PACKAGE_NAMESPACE 
        , CASE
            WHEN L.SFLMA__LICENSED_SEATS__C = 'Site License'
              THEN 'Site'
           ELSE 'Seats'
          END                                       AS LICENSE_SEAT_TYPE    
        , CASE
           WHEN L.SFLMA__SEATS__C > 0
             THEN L.SFLMA__SEATS__C
          ELSE 0               
          END                                       AS SEATS 
        , coalesce(L.SFLMA__USED_LICENSES__C, 0)    AS USED_LICENSES        
        , L.SFLMA__INSTALL_DATE__C                  AS INSTALL_DATE
        , L.UNINSTALL_DATE__C                       AS UNINSTALL_DATE
        , COALESCE(DATEDIFF(MONTH, L.SFLMA__INSTALL_DATE__C, COALESCE(L.UNINSTALL_DATE__C, CURRENT_DATE)),0) AS MONTHS_INSTALLED
        , CASE 
            WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL AND MONTHS_INSTALLED > 12
              THEN 'INSTALLED FOR ' || DATEDIFF(YEAR,  L.SFLMA__INSTALL_DATE__C, COALESCE(L.UNINSTALL_DATE__C, CURRENT_DATE)) || ' YEARS'
            WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL
              THEN 'INSTALLED FOR ' || MONTHS_INSTALLED || ' MONTHS'  
            ELSE 'INSTALL DATE NOT KNOWN'  
          END AS INSTALL_DATE_STRING  
        , CASE
            WHEN UPPER(L.EXPIRATION_DATE__C) <> 'DOES NOT EXPIRE'
              THEN to_Date(L.EXPIRATION_DATE__C)
           else NULL   
          END              AS EXPIRATION_DATE
        , CASE
            WHEN UPPER(L.EXPIRATION_DATE__C) = 'DOES NOT EXPIRE' 
             AND L.UNINSTALL_DATE__C IS NULL
              THEN L.EXPIRATION_DATE__C
            WHEN L.UNINSTALL_DATE__C IS NOT NULL
              THEN 'UNINSTALLED'
            WHEN EXPIRATION_DATE IS NOT NULL
               AND CURRENT_DATE >= EXPIRATION_DATE
                   THEN 'EXPIRED'
            WHEN EXPIRATION_DATE IS NOT NULL            
              THEN 'SET TO EXPIRE'  
            ELSE 'EXPIRATION UNKNOWN'
          END AS EXPIRATION_DATE_STRING                           
        , CASE
                 WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA COMPOSER'
                   THEN 1
                 WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'SALESFORCE CPQ: CONGA QUOTES'
                   THEN 2   
                 WHEN UPPER(L.PACKAGE_NAMEFX__C) = 'CONGA INVOICE GENERATION'  
                   THEN 3
                 ELSE 4  
          END AS PACKAGE_SORT
        , CASE  
            WHEN UPPER(L.SFLMA__LICENSE_STATUS__C) = 'ACTIVE'
              THEN 0
           ELSE 1 
          END AS STATUS_SORT               
        , ROW_NUMBER () OVER (PARTITION BY L.CUSTOMER_ORG_ID__C, P.PRODUCT_LINE ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, PACKAGE_SORT ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PRODUCT_LINE        
        , ROW_NUMBER () OVER (PARTITION BY L.CUSTOMER_ORG_ID__C, L.SFLMA__PACKAGE__C ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PACKAGE_ID
        , ACTIVITY_DATE as LAST_ACTIVITY_DATE
FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT L
LEFT OUTER JOIN        APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE P
                  ON L.PACKAGE_NAMEFX__C = P.PACKAGE_NAME
*/