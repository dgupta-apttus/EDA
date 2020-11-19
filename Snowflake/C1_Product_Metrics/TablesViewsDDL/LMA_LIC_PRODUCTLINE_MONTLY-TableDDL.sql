--DROP TABLE APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY;
--DROP TABLE APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_TEMP; 

--CREATE TABLE APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY 
  ( 
     CUSTOMER_ORG_18             VARCHAR(16777216), 
     PRODUCT_LINE                VARCHAR(16777216), 
     REPORTING_MONTH             DATE, 
     PRIMARY_LICENSE_ID          VARCHAR(16777216), 
     LICENSE_ID_LIST             VARCHAR(16777216), 
     LICENSE_COUNT               INTEGER, 
     CUSTOMER_ORG_15             VARCHAR(16777216), 
     PACKAGE_NAME                VARCHAR(16777216), 
     PACKAGE_LIST                VARCHAR(16777216), 
     PACKAGE_ID                  VARCHAR(16777216), 
     PACKAGE_ID_LIST             VARCHAR(16777216), 
     PACKAGE_COUNT               INTEGER, 
     PACKAGE_VERSION_ID          VARCHAR(16777216), 
     ORG_PACKAGE                 VARCHAR(16777216), 
     STATUS                      VARCHAR(16777216), 
     ORG_STATUS                  VARCHAR(16777216), 
     ACCOUNT_ID                  VARCHAR(16777216), 
     ACCOUNT_NAME                VARCHAR(16777216), 
     IS_SANDBOX                  BOOLEAN, 
     PREDICTED_PACKAGE_NAMESPACE VARCHAR(16777216), 
     LICENSE_SEAT_TYPE           VARCHAR(16777216), 
     SEATS                       INTEGER, 
     USED_LICENSES               INTEGER, 
     INSTALL_DATE                TIMESTAMPTZ, 
     UNINSTALL_DATE              TIMESTAMPTZ, 
     MONTHS_INSTALLED            INTEGER, 
     INSTALL_DATE_STRING         VARCHAR(16777216), 
     LONGEST_INSTALL             INTEGER, 
     EXPIRATION_DATE             DATE, 
     EXPIRATION_DATE_STRING      VARCHAR(16777216), 
     LAST_ACTIVITY_DATE          DATE, 
     SUSPEND_ACCOUNT_BOOL        BOOLEAN, 
     ACCOUNT_SUSPENDED_REASON    VARCHAR(16777216), 
     LICENSE_NAME                VARCHAR(16777216), 
     C1_PRODUCTION_BOOL          BOOLEAN, 
     DATA_SOURCE                 VARCHAR(16777216) 
  ); 

delete from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY; 

  
-- insert redshift history  
  INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY 
SELECT SUBSCRIBER_ORG_ID	                              AS CUSTOMER_ORG_18 
     , PRODUCT_LINE
     , RECORD_TIMESTAMP                      	              AS REPORTING_MONTH
     , SALESFORCE_LICENSE_ID                                  AS PRIMARY_LICENSE_ID
     , SALESFORCE_LICENSE_ID                                  AS LICENSE_ID_LIST
     , -1                                                     AS LICENSE_COUNT -- -1 is for time before there was a method to count   
     , substr(SUBSCRIBER_ORG_ID, 1,15)                        AS CUSTOMER_ORG_15
     , PACKAGE_NAME
     , PACKAGE_NAME                                           AS PACKAGE_LIST	                         
     , NULL                                                   AS PACKAGE_ID
     , NULL                                                   AS PACKAGE_ID_LIST
     , -1                                                     AS PACKAGE_COUNT
     , NULL                                                   AS PACKAGE_VERSION_ID
     , SUBSCRIBER_ORG_ID ||'- NO Package ID'::VARCHAR(255)    AS ORG_PACKAGE 
     , STATUS
     , ORG_STATUS
     , SALESFORCE_ACCOUNT_ID                     AS ACCOUNT_ID
     , SALESFORCE_ACCOUNT_NAME                	 AS ACCOUNT_NAME
     , IS_SANDBOX
     , PREDICTED_PACKAGE_NAMESPACE	
     , LICENSE_SEAT_TYPE
     , SEATS
     , USED_LICENSES
     , INSTALL_DATE
     , UNINSTALL_DATE
     , MONTHS_INSTALLED
     , INSTALL_DATE_STRING
     , MONTHS_INSTALLED                          AS LONGEST_INSTALL     
     , EXPIRATION                                AS EXPIRATION_DATE 
     , EXPIRATION_DATE_STRING
     , '2019-02-01'                              AS LAST_ACTIVITY_DATE
     , 0::BOOLEAN                                AS SUSPEND_ACCOUNT_BOOL
     , NULL                                      AS ACCOUNT_SUSPENDED_REASON
     , NULL                                      AS LICENSE_NAME
     , 1::BOOLEAN                                AS C1_PRODUCTION_BOOL
     , 'C1 Redshift'                             AS DATA_SOURCE      
FROM APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH
WHERE SELECT1 = 1
;

-- manual load 1 from snapshots -- this will be automated for go forward

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
