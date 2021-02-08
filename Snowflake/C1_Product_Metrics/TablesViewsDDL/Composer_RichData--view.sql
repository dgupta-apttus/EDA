-- switch this to a view at some point? or maybe set up daily loads
Create or replace TABLE APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS
as
with set_values as (
        SELECT A.*
              , CASE
                    WHEN UPPER(PACKAGE_NAMESPACE) in ('APXTCONGA4','APXTCFQ','CSFB')
                      THEN (SELECT MAX(PACKAGE_ID) FROM APTTUS_DW.PRODUCT.LICENSE_PACKAGE_PRODUCT_LINE_TWO WHERE MANAGED_PACKAGE_NAMESPACE = UPPER(A.PACKAGE_NAMESPACE))                  
                   ELSE 'NO PACKAGE'
                END AS PACKAGE_ID  
              , CASE 
                    WHEN VERSION_SIMPLE= 'Composer 7' 
                      THEN CONTACT_ID 
                    ELSE USER_ID
                END AS USER_ID_ALL  
              , trunc(to_date(MERGE_TIMESTAMP), 'MONTH') AS REPORT_DATE   
              , trunc(to_date(MERGE_TIMESTAMP), 'DAY') AS MERGE_DATE
        from APTTUS_DW.SF_PRODUCTION.COMPOSER_MERGE_EVENT_LOAD A
)
        SELECT  ACTIVITY, COMBINED_TEMPLATE_FILE_SIZE, CONDUCTOR_BATCH_SIZE, CONDUCTOR_OPERATION_ID, DURATION, DURATION_ATTACHMENTS_ADDED, DURATION_DATA_GATHER, DURATION_DOC_FINISHER, DURATION_DOCUMENTS_UPLOADED, DURATION_EMAIL_TEMPLATES_PROCESSED, DURATION_GOOGLE_DOCS_PROCESSED, DURATION_MERGE_DATA, DURATION_SALESFORCE_OBJECTS_RETRIEVED, DURATION_TEMPLATES_ANALYZED, DURATION_TEMPLATES_GATHERED, DURATION_TOTAL, EMAIL_TEMPLATE_COUNT, EVENT_TYPE, EXCEL_OUTPUT_FILE_SIZE, EXCEL_OUTPUT_FILE_PAGE_COUNT, EXCEL_TEMPLATE_COUNT, GENERATING_PAGE, IP_ADDRESS, IS_CONDUCTOR, IS_FILE_DOWNLOAD, IS_MASS_MERGE, IS_OUTPUT_PDF, IS_POINT_MERGE, IS_SANDBOX_EDITION, IS_SOLUTION_MANAGER, IS_WORKFLOW, MASTER_OBJECT_COUNT, MASTER_OBJECT_NAME, MERGE_TIMESTAMP, NUMBER_OF_TEMPLATES, OTHER_OUTPUT_FILE_SIZE, OTHER_OUTPUT_FILE_PAGE_COUNT, OUTPUT_FILE_TYPE, OUTPUT_FILE_SIZE, OUTPUT_MODE, PACKAGE_NAMESPACE, PDF_FILE_SIZE, PDF_FILE_PAGE_COUNT, PDF_TEMPLATE_COUNT, POWERPOINT_OUTPUT_FILE_SIZE, POWERPOINT_OUTPUT_FILE_PAGE_COUNT, POWERPOINT_TEMPLATE_COUNT, QUERIES_ROW_COUNT, QUERY_ID_COUNT, REPORT_ID_COUNT, REPORTS_ROW_COUNT, SALESFORCE_ORG_ID, SERVER_NAME, TEMPLATE_FILE_TYPE, TOTAL_ROW_COUNT, VERSION, VERSION_SIMPLE, WORD_OUTPUT_FILE_PAGE_COUNT, WORD_OUTPUT_FILE_SIZE, WORD_TEMPLATE_COUNT, MERGE_REGION 
                --, A.USER_ID as USER_ID_8
                --, A.CONTACT_ID AS USER_ID_7
                , MD5(A.CONTACT_ID) as CONTACT_ID_TOKEN
                , MD5(A.USER_ID_ALL) as USER_ID_TOKEN
                , A.USER_TYPE 
                , B.LMA_PACKAGE_ID
                , A.PACKAGE_ID 
                , B.PACKAGE_NAME         
                , C.ACCOUNT_ID AS "Account ID"
                , C.ACCOUNT_NAME AS "Account Name on LMA"                
                , C.PRIMARY_LICENSE_ID AS "License ID"   
                , C.LICENSE_NAME AS "License Name"  
                , COALESCE(C.ACTIVE_SEAT_TYPE, 'Unknown') AS "License Seat Type"
                , COALESCE(C.STATUS, 'NO LICENSE FOUND') AS "Status - License"
                , A.ACCOUNT_ID AS "Account on Activity"
                , A.ACCOUNT_NAME AS "Account Name on Activity"   
                , A.MERGE_DATE as "Merge Date"
                , A.REPORT_DATE as "License Record Date"  
        FROM                    set_values A
        LEFT OUTER JOIN         APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING B
                         ON  A.PACKAGE_ID = B.PACKAGE_ID
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY C
                         ON   A.SALESFORCE_ORG_ID = C.CUSTOMER_ORG_18
                         AND  A.PACKAGE_ID = C.PACKAGE_ID
                         AND  A.REPORT_DATE = C.REPORTING_DATE            
        ;
        