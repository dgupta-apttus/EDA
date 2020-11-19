-- insert redshift history  
INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY 
                (CUSTOMER_ORG, PRODUCT, PRODUCTFAMILY, REPORTING_DATE, CUSTOMER_ORG_15, CUSTOMER_ORG_18, PACKAGE_NAME
                , ACTIVE_PACKAGE_LIST, PACKAGE_ID, PACKAGE_VERSION_ID, ORG_PACKAGE, ACTIVE_SEAT_TYPE, PRIMARY_ROW_SEAT_TYPE
                , ACTIVE_LICENSE_COUNT, ACTIVE_LICENSES_WSEATS, NONPROD_LICENSE_COUNT, SANDBOX_LICENSE_COUNT
                , ACTIVE_SEATS, ACTIVE_USED, NONPROD_SEATS, NONPROD_USED, SANDBOX_SEATS, SANDBOX_USED
                , PRIMARY_ROW_SEATS, PRIMARY_ROW_USED, LONGEST_ACTIVE_INSTALL, ACTIVE_LICENSE_ID_LIST
                , NONPROD_LICENSE_ID_LIST, SANDBOX_LICENSE_ID_LIST, STATUS, ORG_STATUS, ACCOUNT_ID, ACCOUNT_NAME
                , IS_SANDBOX, INSTALL_DATE, UNINSTALL_DATE, MONTHS_INSTALLED, INSTALL_DATE_STRING, EXPIRATION_DATE
                , EXPIRATION_DATE_STRING, LAST_ACTIVITY_DATE, SUSPEND_ACCOUNT_BOOL, ACCOUNT_SUSPENDED_REASON, LICENSE_NAME
                , C1_PRODUCTION_BOOL, PRIMARY_LICENSE_ID, PREDICTED_PACKAGE_NAMESPACE, CRM_SOURCE, PRODUCT_LINE) 
SELECT 
       A.SUBSCRIBER_ORG_ID                                    AS CUSTOMER_ORG
     , B.PRODUCT
     , B.PRODUCTFAMILY
     , date_trunc('MONTH', A.RECORD_TIMESTAMP)                AS REPORTING_DATE
     , substr(SUBSCRIBER_ORG_ID, 1,15)                        AS CUSTOMER_ORG_15
     , SUBSCRIBER_ORG_ID	                              AS CUSTOMER_ORG_18 
     , A.PACKAGE_NAME
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN A.PACKAGE_NAME
        ELSE 'None Active'
       END                                                    AS ACTIVE_PACKAGE_LIST
     , B.PACKAGE_ID
     , NULL                                                   AS PACKAGE_VERSION_ID
     , SUBSCRIBER_ORG_ID ||'- NO Package ID'::VARCHAR(255)    AS ORG_PACKAGE      
     , LICENSE_SEAT_TYPE                                      AS ACTIVE_SEAT_TYPE
     , LICENSE_SEAT_TYPE                                      AS PRIMARY_ROW_SEAT_TYPE
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN 1
        ELSE 0
       END                                                    AS ACTIVE_LICENSE_COUNT
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
          AND LICENSE_SEAT_TYPE = 'Seats'
            THEN 1
        ELSE 0    
       END                                                   AS ACTIVE_LICENSES_WSEATS
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN 1
        ELSE 0        
       END                                                   AS NONPROD_LICENSE_COUNT
     , CASE WHEN  
         IS_SANDBOX = true
            THEN 1
        ELSE 0        
       END                                                   AS SANDBOX_LICENSE_COUNT          
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN SEATS
        ELSE 0
       END                                                   AS ACTIVE_SEATS     
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN USED_LICENSES
        ELSE 0    
       END                                                   AS ACTIVE_USED
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN SEATS
        ELSE 0
       END                                                   AS NONPROD_SEATS                           
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN USED_LICENSES
        ELSE 0
       END                                                   AS NONPROD_USED
     , CASE WHEN  
         IS_SANDBOX = true
            THEN SEATS
        ELSE 0
       END                                                   AS SANDBOX_SEATS
     , CASE WHEN  
         IS_SANDBOX = true
            THEN USED_LICENSES
        ELSE 0
       END                                                   AS SANDBOX_USED           
     , SEATS                                                 AS PRIMARY_ROW_SEATS
     , USED_LICENSES                                         AS PRIMARY_ROW_USED
     , MONTHS_INSTALLED                                      AS LONGEST_ACTIVE_INSTALL
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false 
            THEN SALESFORCE_LICENSE_ID
        ELSE 'None Active'
       END                                                   AS ACTIVE_LICENSE_ID_LIST 
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN SALESFORCE_LICENSE_ID
        ELSE 'None'
       END                                                   AS NONPROD_LICENSE_ID_LIST
     , CASE WHEN  
         IS_SANDBOX = true
            THEN SALESFORCE_LICENSE_ID
        ELSE 'None'
       END                                                   AS SANDBOX_LICENSE_ID_LIST
     , STATUS
     , ORG_STATUS
     , SALESFORCE_ACCOUNT_ID                                 AS ACCOUNT_ID
     , SALESFORCE_ACCOUNT_NAME                               AS ACCOUNT_NAME                        
     , IS_SANDBOX
     , INSTALL_DATE
     , UNINSTALL_DATE
     , MONTHS_INSTALLED
     , INSTALL_DATE_STRING
     , EXPIRATION                                            AS EXPIRATION_DATE
     , EXPIRATION_DATE_STRING
     , NULL AS LAST_ACTIVITY_DATE
     , NULL AS SUSPEND_ACCOUNT_BOOL
     , NULL AS ACCOUNT_SUSPENDED_REASON
     , NULL AS LICENSE_NAME
     , NULL AS C1_PRODUCTION_BOOL
     , SALESFORCE_LICENSE_ID                                 AS PRIMARY_LICENSE_ID                         
     , A.PREDICTED_PACKAGE_NAMESPACE
     , 'Conga1.0'                                            AS CRM_SOURCE
     , A.PRODUCT_LINE
FROM                      APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH A
LEFT OUTER JOIN           APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_C2 B
                  ON A.PACKAGE_NAME = B.PACKAGE_NAME 
WHERE A.SELECT1 = 1
  and A.PACKAGE_NAME is not null
  and A.RECORD_TIMESTAMP <> '2019-11-01 20:22:07' 
;        

INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY 
                (CUSTOMER_ORG, PRODUCT, PRODUCTFAMILY, REPORTING_DATE, CUSTOMER_ORG_15, CUSTOMER_ORG_18, PACKAGE_NAME
                , ACTIVE_PACKAGE_LIST, PACKAGE_ID, PACKAGE_VERSION_ID, ORG_PACKAGE, ACTIVE_SEAT_TYPE, PRIMARY_ROW_SEAT_TYPE
                , ACTIVE_LICENSE_COUNT, ACTIVE_LICENSES_WSEATS, NONPROD_LICENSE_COUNT, SANDBOX_LICENSE_COUNT
                , ACTIVE_SEATS, ACTIVE_USED, NONPROD_SEATS, NONPROD_USED, SANDBOX_SEATS, SANDBOX_USED
                , PRIMARY_ROW_SEATS, PRIMARY_ROW_USED, LONGEST_ACTIVE_INSTALL, ACTIVE_LICENSE_ID_LIST
                , NONPROD_LICENSE_ID_LIST, SANDBOX_LICENSE_ID_LIST, STATUS, ORG_STATUS, ACCOUNT_ID, ACCOUNT_NAME
                , IS_SANDBOX, INSTALL_DATE, UNINSTALL_DATE, MONTHS_INSTALLED, INSTALL_DATE_STRING, EXPIRATION_DATE
                , EXPIRATION_DATE_STRING, LAST_ACTIVITY_DATE, SUSPEND_ACCOUNT_BOOL, ACCOUNT_SUSPENDED_REASON, LICENSE_NAME
                , C1_PRODUCTION_BOOL, PRIMARY_LICENSE_ID, PREDICTED_PACKAGE_NAMESPACE, CRM_SOURCE, PRODUCT_LINE) 
SELECT 
       A.SUBSCRIBER_ORG_ID                                    AS CUSTOMER_ORG
     , B.PRODUCT
     , B.PRODUCTFAMILY
     , '2020-09-01'                                       AS REPORTING_DATE
     , substr(SUBSCRIBER_ORG_ID, 1,15)                        AS CUSTOMER_ORG_15
     , SUBSCRIBER_ORG_ID	                              AS CUSTOMER_ORG_18 
     , A.PACKAGE_NAME
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN A.PACKAGE_NAME
        ELSE 'None Active'
       END                                                    AS ACTIVE_PACKAGE_LIST
     , B.PACKAGE_ID
     , NULL                                                   AS PACKAGE_VERSION_ID
     , SUBSCRIBER_ORG_ID ||'- NO Package ID'::VARCHAR(255)    AS ORG_PACKAGE      
     , LICENSE_SEAT_TYPE                                      AS ACTIVE_SEAT_TYPE
     , LICENSE_SEAT_TYPE                                      AS PRIMARY_ROW_SEAT_TYPE
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN 1
        ELSE 0
       END                                                    AS ACTIVE_LICENSE_COUNT
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
          AND LICENSE_SEAT_TYPE = 'Seats'
            THEN 1
        ELSE 0    
       END                                                   AS ACTIVE_LICENSES_WSEATS
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN 1
        ELSE 0        
       END                                                   AS NONPROD_LICENSE_COUNT
     , CASE WHEN  
         IS_SANDBOX = true
            THEN 1
        ELSE 0        
       END                                                   AS SANDBOX_LICENSE_COUNT          
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN SEATS
        ELSE 0
       END                                                   AS ACTIVE_SEATS     
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false
            THEN USED_LICENSES
        ELSE 0    
       END                                                   AS ACTIVE_USED
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN SEATS
        ELSE 0
       END                                                   AS NONPROD_SEATS                           
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN USED_LICENSES
        ELSE 0
       END                                                   AS NONPROD_USED
     , CASE WHEN  
         IS_SANDBOX = true
            THEN SEATS
        ELSE 0
       END                                                   AS SANDBOX_SEATS
     , CASE WHEN  
         IS_SANDBOX = true
            THEN USED_LICENSES
        ELSE 0
       END                                                   AS SANDBOX_USED           
     , SEATS                                                 AS PRIMARY_ROW_SEATS
     , USED_LICENSES                                         AS PRIMARY_ROW_USED
     , MONTHS_INSTALLED                                      AS LONGEST_ACTIVE_INSTALL
     , CASE WHEN
         UPPER(STATUS) = 'ACTIVE'
          AND ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
          AND IS_SANDBOX = false 
            THEN SALESFORCE_LICENSE_ID
        ELSE 'None Active'
       END                                                   AS ACTIVE_LICENSE_ID_LIST 
     , CASE WHEN 
          IS_SANDBOX = false
          AND (UPPER(STATUS) <> 'ACTIVE'
               OR ORG_STATUS NOT IN ('ACTIVE', 'FREE', 'SIGNING_UP')
              )
            THEN SALESFORCE_LICENSE_ID
        ELSE 'None'
       END                                                   AS NONPROD_LICENSE_ID_LIST
     , CASE WHEN  
         IS_SANDBOX = true
            THEN SALESFORCE_LICENSE_ID
        ELSE 'None'
       END                                                   AS SANDBOX_LICENSE_ID_LIST
     , STATUS
     , ORG_STATUS
     , SALESFORCE_ACCOUNT_ID                                 AS ACCOUNT_ID
     , SALESFORCE_ACCOUNT_NAME                               AS ACCOUNT_NAME                        
     , IS_SANDBOX
     , INSTALL_DATE
     , UNINSTALL_DATE
     , MONTHS_INSTALLED
     , INSTALL_DATE_STRING
     , EXPIRATION                                            AS EXPIRATION_DATE
     , EXPIRATION_DATE_STRING
     , NULL AS LAST_ACTIVITY_DATE
     , NULL AS SUSPEND_ACCOUNT_BOOL
     , NULL AS ACCOUNT_SUSPENDED_REASON
     , NULL AS LICENSE_NAME
     , NULL AS C1_PRODUCTION_BOOL
     , SALESFORCE_LICENSE_ID                                 AS PRIMARY_LICENSE_ID                         
     , A.PREDICTED_PACKAGE_NAMESPACE
     , 'Conga1.0'                                            AS CRM_SOURCE
     , A.PRODUCT_LINE
FROM                      APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH A
LEFT OUTER JOIN           APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_C2 B
                  ON A.PACKAGE_NAME = B.PACKAGE_NAME 
WHERE A.SELECT1 = 1
  and A.PACKAGE_NAME is not null
  and A.RECORD_TIMESTAMP = '2020-08-31 00:00:00' 
;    

SELECT COUNT(*), REPORTING_DATE, CRM_SOURCE
FROM APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY
group by REPORTING_DATE, CRM_SOURCE
;

SELECT count(*), A.RECORD_TIMESTAMP, date_trunc('MONTH', A.RECORD_TIMESTAMP) AS REPORTING_DATE 
FROM                      APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH A
WHERE A.SELECT1 = 1
  and A.PACKAGE_NAME is not null
  and A.RECORD_TIMESTAMP <> '2019-11-01 20:22:07'
group by A.RECORD_TIMESTAMP 
;  


     , PRODUCT_LINE
     , RECORD_TIMESTAMP                      	              AS REPORTING_MONTH
     , SALESFORCE_LICENSE_ID                                  AS PRIMARY_LICENSE_ID
     , SALESFORCE_LICENSE_ID                                  AS LICENSE_ID_LIST
     , -1                                                     AS LICENSE_COUNT -- -1 is for time before there was a method to count   
     
     , PACKAGE_NAME
     , PACKAGE_NAME                                           AS PACKAGE_LIST	                         
     , NULL                                                   AS PACKAGE_ID
     , NULL                                                   AS PACKAGE_ID_LIST
     , -1                                                     AS PACKAGE_COUNT


     , STATUS
     , ORG_STATUS
     , SALESFORCE_ACCOUNT_ID                     AS ACCOUNT_ID
     , SALESFORCE_ACCOUNT_NAME                	 AS ACCOUNT_NAME
     , IS_SANDBOX
     , PREDICTED_PACKAGE_NAMESPACE	
     , 
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

