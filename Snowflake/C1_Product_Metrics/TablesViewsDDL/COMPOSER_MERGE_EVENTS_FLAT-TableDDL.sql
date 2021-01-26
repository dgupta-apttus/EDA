-- switch this to a view at some point? or maybe set up daily loads
Create or replace TABLE APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS_FLAT
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
        SELECT  ACTIVITY, COMBINED_TEMPLATE_FILE_SIZE, CONDUCTOR_BATCH_SIZE, CONDUCTOR_OPERATION_ID, DURATION, DURATION_ATTACHMENTS_ADDED, DURATION_DATA_GATHER, DURATION_DOC_FINISHER, DURATION_DOCUMENTS_UPLOADED, DURATION_EMAIL_TEMPLATES_PROCESSED, DURATION_GOOGLE_DOCS_PROCESSED, DURATION_MERGE_DATA, DURATION_SALESFORCE_OBJECTS_RETRIEVED, DURATION_TEMPLATES_ANALYZED, DURATION_TEMPLATES_GATHERED, DURATION_TOTAL, EMAIL_TEMPLATE_COUNT, EXCEL_OUTPUT_FILE_SIZE, EXCEL_OUTPUT_FILE_PAGE_COUNT, EXCEL_TEMPLATE_COUNT, GENERATING_PAGE, IP_ADDRESS, IS_CONDUCTOR, IS_FILE_DOWNLOAD, IS_MASS_MERGE, IS_OUTPUT_PDF, IS_POINT_MERGE, IS_SOLUTION_MANAGER, IS_WORKFLOW, MASTER_OBJECT_COUNT, MASTER_OBJECT_NAME, MERGE_TIMESTAMP, NUMBER_OF_TEMPLATES, OTHER_OUTPUT_FILE_SIZE, OTHER_OUTPUT_FILE_PAGE_COUNT, OUTPUT_FILE_TYPE, OUTPUT_FILE_SIZE, OUTPUT_MODE, PACKAGE_NAMESPACE, PDF_FILE_SIZE, PDF_FILE_PAGE_COUNT, PDF_TEMPLATE_COUNT, POWERPOINT_OUTPUT_FILE_SIZE, POWERPOINT_OUTPUT_FILE_PAGE_COUNT, POWERPOINT_TEMPLATE_COUNT, QUERIES_ROW_COUNT, QUERY_ID_COUNT, REPORT_ID_COUNT, REPORTS_ROW_COUNT, SALESFORCE_ORG_ID, SERVER_NAME, TEMPLATE_FILE_TYPE, TOTAL_ROW_COUNT, VERSION, VERSION_SIMPLE, WORD_OUTPUT_FILE_PAGE_COUNT, WORD_OUTPUT_FILE_SIZE, WORD_TEMPLATE_COUNT, MERGE_REGION 
                , EVENT_TYPE
                , CASE EVENT_TYPE
                    WHEN 'Workflow' THEN 'Trigger'
                    WHEN 'PointMerge' THEN 'Composer'
                    WHEN 'Conductor' THEN 'Batch'
                    WHEN 'MassMerge' THEN 'Mail Merge'
                   ELSE 'Other' 
                  END                AS "Event Type" 
                , A.USER_ID as USER_ID_8
                , A.CONTACT_ID AS USER_ID_7
                , A.USER_ID_ALL
                , A.USER_TYPE 
                , B.LMA_PACKAGE_ID
                , A.PACKAGE_ID 
                , B.PACKAGE_NAME         
                , COALESCE(E.ACCOUNT_ID, A.ACCOUNT_ID) AS "Account ID"
                , E.ACCOUNT_NAME AS "Account Name on LMA"                
                , COALESCE(E.LICENSE_ID, 'NOT FOUND') AS "License ID"   
                , E.LICENSE_NAME AS "License Name"  
                , COALESCE(E.LICENSE_SEAT_TYPE, 'Unknown') AS "License Seat Type"
                , CASE 
                   WHEN E.IS_SANDBOX = false
                    AND UPPER(E.STATUS) = 'ACTIVE'
                    AND E.ORG_STATUS IN ('ACTIVE', 'FREE')
                    THEN 'License Active'
                   WHEN E.LICENSE_ID IS NULL
                    THEN 'License Not Found'
                   ELSE 'License Not Active'
                  END                                     AS "License Status"    
                , CASE                      
                   WHEN E.ACCOUNT_ID IS NOT NULL
                    THEN ' w/ Acc'
                   ELSE ''
                  END                                     AS "Account Found"   
                , CASE
                    WHEN A.IS_SANDBOX_EDITION = false
                     THEN 'Production'
                   ELSE 'Sandbox'  
                  END                                     AS "Activity Type"   
                ,  "Activity Type" || ' - ' || "License Status" || "Account Found" AS "Status Desciption"                                       
                , E.STATUS                           AS "LMA Status"
                , E.ORG_STATUS                       AS "Org Status" 
                , E.IS_SANDBOX                       AS "Is Sandbox - License" 
                , A.IS_SANDBOX_EDITION               AS "Is Sandbox - Activity" 
                , A.ACCOUNT_ID AS "Account on Activity"
                , A.ACCOUNT_NAME AS "Account Name on Activity"   
                , A.MERGE_DATE as "Merge Date"
                , A.REPORT_DATE as "Merge Month" 
                , 1 as "Merge Count"
                , COALESCE(F."Account Name", 'NOT FOUND') as "Account Name" 
                , F."Account Type"
                , F."Geo"
                , F."Region"
                , F."Segment"
                , F."Establishing Partner"
                , F."Next Renewal"
                , F."Customer Since"
                , F."Industry"
                , F."SIC4"
                , F."Netsuite ID"                     
        FROM                    set_values A
        LEFT OUTER JOIN         APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING B
                         ON  A.PACKAGE_ID = B.PACKAGE_ID
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT E
                         ON   A.SALESFORCE_ORG_ID = E.CUSTOMER_ORG_18
                         AND  A.PACKAGE_ID = E.PACKAGE_ID  
                         AND  E.SELECT1_FOR_PACKAGE_ID = 1              
        LEFT OUTER JOIN         APTTUS_DW.SF_PRODUCTION."Account_C2_FL" F
                         ON E.ACCOUNT_ID = F."Account ID"                                      
;

select  "LMA Status"
       , "Org Status" 
       , "Is Sandbox - License" 
       , "Is Sandbox - Activity"       
       , count(*) 
from APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS_FLAT
group by "LMA Status"
       , "Org Status" 
       , "Is Sandbox - License" 
       , "Is Sandbox - Activity"       
;

select
        "Status Desciption" 
       , count(*) 
from APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS_FLAT
group by "Status Desciption"  
;

SELECT distinct EVENT_TYPE
from APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS_FLAT
;

 select CASE EVENT_TYPE
                    WHEN 'Workflow' THEN 'Trigger'
                    WHEN 'PointMerge' THEN 'Button Click'
                    WHEN 'Conductor' THEN 'Batch'
                    WHEN 'MassMerge' THEN 'MassMerge'
                   ELSE 'Other' 
                  END                AS "Event Type" 
 from APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS_FLAT
;  

SELECT MAX(MERGE_TIMESTAMP) from    APTTUS_DW.SF_PRODUCTION.COMPOSER_MERGE_EVENT_LOAD
;     
SELECT "Merge Date", SALESFORCE_ORG_ID 
 from APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS_FLAT  
WHERE "Account Name" like 'Altu%' 
order by 1 desc; 

SELECT "Merge Date"
 from APTTUS_DW.PRODUCT.COMPOSER_MERGE_EVENTS_FLAT
WHERE SALESFORCE_ORG_ID IN ('00D41000000doeEEAQ')    
;

Select CUSTOMER_ORG_18
from APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT
where CRM_SOURCE = 'Apttus1.0'
;