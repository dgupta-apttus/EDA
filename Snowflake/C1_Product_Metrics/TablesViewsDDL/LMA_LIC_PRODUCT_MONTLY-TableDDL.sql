DROP TABLE APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY;

CREATE TABLE APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY 
   ( CUSTOMER_ORG VARCHAR(16777216)
   , PRODUCT VARCHAR(16777216)
   , REPORTING_MONTH DATE
   , CUSTOMER_ORG_15 VARCHAR(16777216)
   , CUSTOMER_ORG_18 VARCHAR(16777216)
   , PACKAGE_NAME VARCHAR(16777216)
   , ACTIVE_PACKAGE_LIST VARCHAR(16777216)
   , PACKAGE_ID VARCHAR(16777216)
   , PACKAGE_ID_LIST VARCHAR(16777216)
   , PACKAGE_COUNT INTEGER
   , PACKAGE_VERSION_ID VARCHAR(16777216)
   , ORG_PACKAGE VARCHAR(16777216)
   , ACTIVE_SEAT_TYPE VARCHAR(16777216)
   , PRIMARY_ROW_SEAT_TYPE VARCHAR(16777216)
   , ACTIVE_LICENSE_COUNT INTEGER
   , ACTIVE_LICENSES_WSEATS INTEGER
   , NONPROD_LICENSE_COUNT INTEGER
   , SANDBOX_LICENSE_COUNT INTEGER
   , ACTIVE_SEATS INTEGER
   , ACTIVE_USED INTEGER
   , NONPROD_SEATS INTEGER
   , NONPROD_USED INTEGER
   , SANDBOX_SEATS INTEGER
   , SANDBOX_USED INTEGER
   , PRIMARY_ROW_SEATS INTEGER
   , PRIMARY_ROW_USED INTEGER
   , LONGEST_ACTIVE_INSTALL INTEGER
   , ACTIVE_LICENSE_ID_LIST VARCHAR(16777216)
   , NONPROD_LICENSE_ID_LIST VARCHAR(16777216)
   , SANDBOX_LICENSE_ID_LIST VARCHAR(16777216)
   , STATUS VARCHAR(16777216)
   , ORG_STATUS VARCHAR(16777216)
   , ACCOUNT_ID VARCHAR(16777216)
   , ACCOUNT_NAME VARCHAR(16777216)
   , IS_SANDBOX BOOLEAN
   , INSTALL_DATE TIMESTAMPTZ
   , UNINSTALL_DATE TIMESTAMPTZ
   , MONTHS_INSTALLED NUMBER
   , INSTALL_DATE_STRING VARCHAR(16777216)
   , EXPIRATION_DATE DATE
   , EXPIRATION_DATE_STRING VARCHAR(16777216)
   , LAST_ACTIVITY_DATE DATE
   , SUSPEND_ACCOUNT_BOOL BOOLEAN
   , ACCOUNT_SUSPENDED_REASON VARCHAR(16777216)
   , LICENSE_NAME VARCHAR(16777216)
   , C1_PRODUCTION_BOOL BOOLEAN
   , PRIMARY_LICENSE_ID VARCHAR(16777216)
   , PREDICTED_PACKAGE_NAMESPACE VARCHAR(16777216)
   , CRM_SOURCE VARCHAR(255)
   , PRODUCT_LINE VARCHAR(16777216)
);

--delete from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY; 

/*  
-- insert redshift history  
  INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY 
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

INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY 
WITH LISTS AS (
--blah blah blah  
;  

-- move redshift data plus one month done for september to improved method that breaks out sandbox and nonproduction -- sandbox and non-prod set to zero and only primary rows that are active brought over
INSERT INTO APTTUS_DW.PRODUCT.LMA_LIC_PRODUCT_MONTHLY 
            (CUSTOMER_ORG, 
             PRODUCT, 
             REPORTING_MONTH, 
             CUSTOMER_ORG_15, 
             CUSTOMER_ORG_18, 
             PACKAGE_NAME, 
             ACTIVE_PACKAGE_LIST, 
             PACKAGE_ID, 
             PACKAGE_ID_LIST, 
             PACKAGE_COUNT, 
             PACKAGE_VERSION_ID, 
             ORG_PACKAGE, 
             ACTIVE_SEAT_TYPE, 
             PRIMARY_ROW_SEAT_TYPE, 
             ACTIVE_LICENSE_COUNT, 
             ACTIVE_LICENSES_WSEATS, 
             NONPROD_LICENSE_COUNT, 
             SANDBOX_LICENSE_COUNT, 
             ACTIVE_SEATS, 
             ACTIVE_USED, 
             NONPROD_SEATS, 
             NONPROD_USED, 
             SANDBOX_SEATS, 
             SANDBOX_USED, 
             PRIMARY_ROW_SEATS, 
             PRIMARY_ROW_USED, 
             LONGEST_ACTIVE_INSTALL, 
             ACTIVE_LICENSE_ID_LIST, 
             NONPROD_LICENSE_ID_LIST, 
             SANDBOX_LICENSE_ID_LIST, 
             STATUS, 
             ORG_STATUS, 
             ACCOUNT_ID, 
             ACCOUNT_NAME, 
             IS_SANDBOX, 
             INSTALL_DATE, 
             UNINSTALL_DATE, 
             MONTHS_INSTALLED, 
             INSTALL_DATE_STRING, 
             EXPIRATION_DATE, 
             EXPIRATION_DATE_STRING, 
             LAST_ACTIVITY_DATE, 
             SUSPEND_ACCOUNT_BOOL, 
             ACCOUNT_SUSPENDED_REASON, 
             LICENSE_NAME, 
             C1_PRODUCTION_BOOL, 
             PRIMARY_LICENSE_ID, 
             PREDICTED_PACKAGE_NAMESPACE, 
             CRM_SOURCE, 
             PRODUCT_LINE) 
SELECT	 CUSTOMER_ORG_18 as CUSTOMER_ORG
	, PRODUCT_LINE AS PRODUCT
	, REPORTING_MONTH
	, CUSTOMER_ORG_15
	, CUSTOMER_ORG_18
	, PACKAGE_NAME
	, PACKAGE_LIST AS ACTIVE_PACKAGE_LIST
	, PACKAGE_ID
	, PACKAGE_ID_LIST
	, PACKAGE_COUNT
	, PACKAGE_VERSION_ID
	, ORG_PACKAGE
	, LICENSE_SEAT_TYPE AS ACTIVE_SEAT_TYPE
	, LICENSE_SEAT_TYPE AS PRIMARY_ROW_SEAT_TYPE
	, LICENSE_COUNT AS ACTIVE_LICENSE_COUNT
	, CASE
	    WHEN LICENSE_SEAT_TYPE = 'Seats'
	      THEN 1
	   ELSE 0   
	  END AS ACTIVE_LICENSES_WSEATS
	, 0 AS NONPROD_LICENSE_COUNT
	, 0 AS SANDBOX_LICENSE_COUNT
	, SEATS AS ACTIVE_SEATS
	, USED_LICENSES AS ACTIVE_USED
	, 0 AS NONPROD_SEATS
	, 0 AS NONPROD_USED
	, 0 AS SANDBOX_SEATS
	, 0 AS SANDBOX_USED
	, SEATS AS PRIMARY_ROW_SEATS
	, USED_LICENSES AS PRIMARY_ROW_USED
	, LONGEST_INSTALL AS LONGEST_ACTIVE_INSTALL
	, LICENSE_ID_LIST AS ACTIVE_LICENSE_ID_LIST
	, NULL AS NONPROD_LICENSE_ID_LIST
	, NULL AS SANDBOX_LICENSE_ID_LIST
	, STATUS
	, ORG_STATUS
	, ACCOUNT_ID
	, ACCOUNT_NAME
	, IS_SANDBOX
	, INSTALL_DATE
	, UNINSTALL_DATE
	, MONTHS_INSTALLED
	, INSTALL_DATE_STRING
	, EXPIRATION_DATE
	, EXPIRATION_DATE_STRING
	, LAST_ACTIVITY_DATE
	, SUSPEND_ACCOUNT_BOOL
	, ACCOUNT_SUSPENDED_REASON
	, LICENSE_NAME
	, C1_PRODUCTION_BOOL
	, PRIMARY_LICENSE_ID
	, PREDICTED_PACKAGE_NAMESPACE
	, DATA_SOURCE AS CRM_SOURCE
	, PRODUCT_LINE 
FROM   APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_MONTHLY
where STATUS = 'Active'
  and ORG_STATUS IN ('ACTIVE', 'FREE', 'SIGNING_UP')
  and IS_SANDBOX = false           
;

