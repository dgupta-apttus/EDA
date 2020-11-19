
select *
FROM APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21"	
where "Snapshot Date" = '2020-09-29'
;
SELECT *
FROM APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH
WHERE SUBSCRIBER_ORG_ID IN ('00D2X000000Txd2UAC')
--order by LICENSE_ID, RECORD_timestamp
;
SELECT *
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT
WHERE --SALESFORCE_ORGID__C IN ('00D2X000000Txd2UAC')
      ID IN ('a021T00000yTFS8QAO')
;

SELECT distinct EXPIRATION_DATE__C, SFLMA__EXPIRATION_DATE__C
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT
;

SELECT count(*), SFLMA__SEATS__C, SFLMA__LICENSED_SEATS__C
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT
group by SFLMA__SEATS__C, SFLMA__LICENSED_SEATS__C
;

SELECT count(*), SFLMA__LICENSE_STATUS__C, SFLMA__ORG_STATUS__C--, SFLMA__STATUS__C, SFLMA__ORG_STATUS_FORMULA__C
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT
group by SFLMA__LICENSE_STATUS__C, SFLMA__ORG_STATUS__C--, SFLMA__STATUS__C, SFLMA__ORG_STATUS_FORMULA__C
;

SELECT distinct LICENSE_SEAT_TYPE
FROM APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH
;


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
;