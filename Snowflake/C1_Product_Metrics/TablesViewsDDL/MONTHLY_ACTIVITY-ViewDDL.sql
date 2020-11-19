
--DROP VIEW APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY;

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY 
COMMENT = 'UNION MONTHLY ACTIVITY FROM 3 OBJECTS, FMA, COMPOSER, AND PIPELINED'
AS 
WITH union_3_sources as (
        SELECT ORG_SOURCE
                , SOURCE_ORG_ID
                , ACTIVITY_YEAR
                , ACTIVITY_MONTH
                , ACTIVITY_MONTH_DATE
                , PRODUCT_LINE
                , ACTIVITY_COUNT
                , UNIQUE_USERS
                , NULL AS LICENSE_ID
                , IS_SANDBOX_EDITION
                , CASE  
                    WHEN PACKAGE_NAMESPACE IS NOT NULL
                      THEN PACKAGE_NAMESPACE 
                   ELSE 'NULL COMPOSER'
                  END AS PACKAGE_NAMESPACE 
                , ACTIVITY_ACCOUNT_ID
                , ACTIVITY_ACCOUNT_NAME
                , SERVICE_EVENT_MERGES
                , PERCENT_SERVICE_EVENTS
        FROM
                APTTUS_DW.PRODUCT.COMPOSER_MONTHLY_ACTIVITY
        UNION
        SELECT ORG_SOURCE
                , SOURCE_ORG_ID
                , ACTIVITY_YEAR
                , ACTIVITY_MONTH
                , ACTIVITY_MONTH_DATE
                , PRODUCT_LINE
                , ACTIVITY_COUNT
                , UNIQUE_USERS
                , LICENSE_ID
                , 0::BOOLEAN AS IS_SANDBOX_EDITION
                , NULL AS PACKAGE_NAMESPACE
                , NULL AS ACTIVITY_ACCOUNT_ID
                , NULL AS ACTIVITY_ACCOUNT_NAME
                , NULL AS SERVICE_EVENT_MERGES
                , NULL AS PERCENT_SERVICE_EVENTS
        FROM
                APTTUS_DW.PRODUCT.FMA_MONTHLY_ACTIVITY
        UNION
        SELECT ORG_SOURCE
                , SOURCE_ORG_ID
                , ACTIVITY_YEAR
                , ACTIVITY_MONTH
                , ACTIVITY_MONTH_DATE
                , PRODUCT_LINE
                , ACTIVITY_COUNT
                , UNIQUE_USERS
                , NULL AS LICENSE_ID
                , IS_SANDBOX_EDITION
                , NULL AS PACKAGE_NAMESPACE 
                , ACTIVITY_ACCOUNT_ID
                , ACTIVITY_ACCOUNT_NAME
                , NULL AS SERVICE_EVENT_MERGES
                , NULL AS PERCENT_SERVICE_EVENTS	
        FROM
                APTTUS_DW.PRODUCT.PIPELINED_MONTHLY_ACTIVITY
)
        SELECT    ORG_SOURCE
                , SOURCE_ORG_ID
                , ACTIVITY_YEAR
                , ACTIVITY_MONTH
                , ACTIVITY_MONTH_DATE
                , PRODUCT_LINE
                , ACTIVITY_COUNT
                , UNIQUE_USERS
                , LICENSE_ID
                , IS_SANDBOX_EDITION
                , UPPER(PACKAGE_NAMESPACE) as PACKAGE_NAMESPACE
                , ACTIVITY_ACCOUNT_ID
                , ACTIVITY_ACCOUNT_NAME
                , SERVICE_EVENT_MERGES
                , PERCENT_SERVICE_EVENTS
                , CASE
                    WHEN UPPER(PACKAGE_NAMESPACE) in ('APXTCONGA4','APXTCFQ','CSFB')
                      THEN (SELECT MAX(PACKAGE_ID) FROM APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_C2 WHERE MANAGED_PACKAGE_NAMESPACE = UPPER(A.PACKAGE_NAMESPACE))                  
                    WHEN PRODUCT_LINE NOT IN ('Conga Composer', 'Conga Contracts', 'Conga Collaborate')
                      THEN (SELECT MAX(PACKAGE_ID) FROM APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_C2 WHERE PRODUCT_LINE = A.PRODUCT_LINE) 
                   ELSE 'NO PACKAGE: ' || PRODUCT_LINE
                  END AS PACKAGE_ID    
                , 'Conga1.0' as CRM_SOURCE                                  
        FROM union_3_sources A              
;

/* --test scripts
select distinct product_line, package_id
from APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
;

select count(*), ACTIVITY_MONTH_DATE
from APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
where package_namespace = 'NULL COMPOSER'
group by ACTIVITY_MONTH_DATE
;
select count(*)
from APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
where PRODUCT_LINE = 'Conga Composer'
--where PACKAGE_NAMESPACE = 'NULL COMPOSER'
  and ACTIVITY_MONTH_DATE = '2020-09-01'
;

SELECT COUNT(*), PRODUCT_LINE, min(ACTIVITY_MONTH_DATE), MAX(ACTIVITY_MONTH_DATE)
FROM APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY
GROUP BY PRODUCT_LINE
;
*/