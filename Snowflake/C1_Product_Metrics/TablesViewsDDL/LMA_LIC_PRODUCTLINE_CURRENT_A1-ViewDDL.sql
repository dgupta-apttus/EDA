--DROP VIEW APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT;

--CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
--COMMENT = 'Compute fields and windows for further License / Productline processing.  Current view'
--AS 
with temp_inner as (
SELECT    L.ID as LICENSE_ID
--        ,                    AS CUSTOMER_ORG_18
        , L.SFLMA__SUBSCRIBER_ORG_ID__C             AS CUSTOMER_ORG_15  
        , P.PRODUCTFAMILY
        , P.PRODUCT
        , L.PACKAGE_NAME__C                         AS PACKAGE_NAME
        , L.SFLMA__PACKAGE__C                       AS PACKAGE_ID
        , L.SFLMA__PACKAGE_VERSION__C               AS PACKAGE_VERSION_ID
        , CUSTOMER_ORG_15 || '-' || PACKAGE_ID      AS ORG_PACKAGE
        , L.SFLMA__LICENSE_STATUS__C                AS STATUS
        , COALESCE(SFLMA__ORG_STATUS__C, 'Unknown') AS ORG_STATUS 
        , L.SFLMA__ACCOUNT__C                       AS ACCOUNT_ID
        , L.ACCOUNT_NAME__C                         AS ACCOUNT_NAME  
        , L.SFLMA__IS_SANDBOX__C                    AS IS_SANDBOX
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
        , NULL                                      AS UNINSTALL_DATE -- CLMCPQ does appear to have an uninstall date
        , CASE
            WHEN UPPER(L.SFLMA__EXPIRATION_DATE__C) <> 'DOES NOT EXPIRE'
              THEN to_Date(L.SFLMA__EXPIRATION__C)
           else NULL   
          END                                       AS EXPIRATION_DATE
        , CASE
            WHEN UPPER(L.SFLMA__EXPIRATION_DATE__C) = 'DOES NOT EXPIRE' 
             AND L.SFLMA__LICENSE_STATUS__C NOT IN ('Uninstalled')
              THEN UPPER(L.SFLMA__EXPIRATION_DATE__C)
            WHEN L.SFLMA__LICENSE_STATUS__C IN ('Uninstalled')
              THEN 'UNINSTALLED'
            WHEN EXPIRATION_DATE IS NOT NULL
               AND CURRENT_DATE >= EXPIRATION_DATE
                   THEN 'EXPIRED'
            WHEN UPPER(L.SFLMA__EXPIRATION_DATE__C) <> 'DOES NOT EXPIRE'            
              THEN 'SET TO EXPIRE'  
            ELSE 'EXPIRATION UNKNOWN'
          END                                      AS EXPIRATION_DATE_STRING    
        , CASE 
            WHEN EXPIRATION_DATE IS NOT NULL
               AND CURRENT_DATE >= EXPIRATION_DATE
               AND L.SFLMA__LICENSE_STATUS__C NOT IN ('Uninstalled')
              THEN COALESCE(DATEDIFF(MONTH, L.SFLMA__INSTALL_DATE__C, CURRENT_DATE),0)               
            WHEN L.SFLMA__LICENSE_STATUS__C NOT IN ('Uninstalled') 
              THEN COALESCE(DATEDIFF(MONTH, L.SFLMA__INSTALL_DATE__C, CURRENT_DATE),0) 
            ELSE 0
          END AS MONTHS_INSTALLED    
        , CASE 
            WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL AND MONTHS_INSTALLED > 12
              THEN 'INSTALLED FOR ' || DATEDIFF(YEAR,  L.SFLMA__INSTALL_DATE__C, CURRENT_DATE) || ' YEARS'
            WHEN  L.SFLMA__INSTALL_DATE__C IS NOT NULL
              THEN 'INSTALLED FOR ' || MONTHS_INSTALLED || ' MONTHS'  
            ELSE 'INSTALL DATE NOT KNOWN'  
          END                                       AS INSTALL_DATE_STRING  
--        , L.SUSPEND_ACCOUNT__C                     AS SUSPEND_ACCOUNT_BOOL
--        , L.ACCOUNT_SUSPENDED__C                   AS ACCOUNT_SUSPENDED_REASON
        , L.NAME                                   AS LICENSE_NAME
--        , L.PRODUCTION__C                          AS C1_PRODUCTION_BOOL -- not sure this meaningful!
--        , CASE END                               AS PACKAGE_SORT
        , CASE  
            WHEN UPPER(L.SFLMA__LICENSE_STATUS__C) = 'ACTIVE'
             AND EXPIRATION_DATE_STRING <> 'EXPIRED' 
              THEN 0
            WHEN UPPER(L.SFLMA__LICENSE_STATUS__C) = 'ACTIVE'
              THEN 1  
           ELSE 2 
          END                                      AS STATUS_SORT               
        , CASE  
            WHEN UPPER(ORG_STATUS) IN ('DELETED', 'PENDING_DELETE', 'SUSPENDED')
--              OR SUSPEND_ACCOUNT__C = TRUE
              THEN 3          
            WHEN UPPER(ORG_STATUS) = 'ACTIVE'
              THEN 0
            WHEN UPPER(ORG_STATUS) = 'FREE'
              THEN 1
           ELSE 2 
          END                                      AS ORG_STATUS_SORT  
        , ROW_NUMBER () OVER (PARTITION BY L.SFLMA__SUBSCRIBER_ORG_ID__C, P.PRODUCT ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, ORG_STATUS_SORT ASC, PACKAGE_NAME ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PRODUCT        
        , ROW_NUMBER () OVER (PARTITION BY L.SFLMA__SUBSCRIBER_ORG_ID__C, L.SFLMA__PACKAGE__C ORDER BY IS_SANDBOX ASC, STATUS_SORT ASC, ORG_STATUS_SORT ASC, INSTALL_DATE DESC) AS SELECT1_FOR_PACKAGE_ID
        , ACTIVITY_DATE                            AS LAST_ACTIVITY_DATE
FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT L
LEFT OUTER JOIN        APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_FAMILY P
                  ON L.SFLMA__PACKAGE__C = P.PACKAGEID
WHERE L.ISDELETED = FALSE  
  AND L.SFLMA__SUBSCRIBER_ORG_ID__C is not null
  AND L.SFLMA__PACKAGE__C  is not null               
)

select *  from temp_inner 
--where CUSTOMER_ORG_15 IN ('00D7F000004c2q2','00D7F000005lJQ2','00D8A0000005Sjn','00D8A000000D2Wi')
--where SELECT1_FOR_PRODUCT <> SELECT1_FOR_PACKAGE_ID 
--order by CUSTOMER_ORG_15, PRODUCT, PACKAGE_NAME,  SELECT1_FOR_PRODUCT, SELECT1_FOR_PACKAGE_ID 
;

select distinct SFLMA__EXPIRATION_DATE__C
from APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT
order by 1 desc

;
SELECT count(*), PACKAGE_NAME, STATUS, ORG_STATUS, IS_SANDBOX, STATUS_SORT, ORG_STATUS_SORT
from temp_inner
where SELECT1_FOR_PACKAGE_NAME = 1
group by PACKAGE_NAME, STATUS, ORG_STATUS, IS_SANDBOX, STATUS_SORT, ORG_STATUS_SORT
order by IS_SANDBOX asc, STATUS_SORT, ORG_STATUS_SORT, 1 desc
;

select count(distinct ACCOUNT_ID)
from temp_inner
where SELECT1_FOR_PACKAGE_NAME = 1
 and STATUS = 'Active' and ORG_STATUS = 'ACTIVE'
; 

select SELECT1_FOR_PACKAGE_NAME, count(*)
from temp_inner
group by SELECT1_FOR_PACKAGE_NAME
;      



select count(*)
from
(
SELECT COUNT(*), SFLMA__SUBSCRIBER_ORG_ID__C, SFLMA__PACKAGE__C
FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT L
where SFLMA__SUBSCRIBER_ORG_ID__C is not null
group by SFLMA__SUBSCRIBER_ORG_ID__C, SFLMA__PACKAGE__C
)
;

SELECT COUNT(*), SFLMA__IS_SANDBOX__C, SFLMA__SUBSCRIBER_ORG_IS_SANDBOX__C
FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT L
where SFLMA__SUBSCRIBER_ORG_ID__C is not null
group by SFLMA__IS_SANDBOX__C, SFLMA__SUBSCRIBER_ORG_IS_SANDBOX__C
;

SELECT COUNT(*), SFLMA__STATUS__C, SFLMA__ORG_STATUS__C
FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT L
--where SFLMA__SUBSCRIBER_ORG_ID__C is not null
group by SFLMA__STATUS__C, SFLMA__ORG_STATUS__C
;
            
select *
FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT L
where SFLMA__SUBSCRIBER_ORG_ID__C is not null
--  and NOT UPPER(OTHER_USERS_DESCRIPTION__C) like '%DUMMY%' 
;

select count(*)
       , 
       , L.SFLMA__LICENSE_STATUS__C                AS STATUS
       , COALESCE(SFLMA__ORG_STATUS__C, 'Unknown') AS ORG_STATUS
FROM                   APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT L
group by L.SFLMA__LICENSE_STATUS__C, SFLMA__ORG_STATUS__C   
;


select count(*) from APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__LICENSE__C
where SFLMA__STATUS__C = 'Active' and SFLMA__ORG_STATUS__C = 'ACTIVE' and SFLMA__SUBSCRIBER_ORG_ID__C is not null;
;

select count(*)
FROM APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__LICENSE__C L --1458929
WHERE 
L.SFLMA__SUBSCRIBER_ORG_ID__C IS NOT NULL
AND L.SFLMA__ORG_STATUS__C ='ACTIVE'
;

