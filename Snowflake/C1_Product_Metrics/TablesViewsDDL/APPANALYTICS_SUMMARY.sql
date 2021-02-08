CREATE or replace VIEW APTTUS_DW.PRODUCT.APPANALYTICS_SUMMARY 
COMMENT = 'Product App Analytics Summary Data from a1 and c1
' 
AS 
        SELECT  'Apttus1.0' as CRM_SOURCE
              , to_date("MONTH" || '-' || '01' ) AS "DATE"
              , ORGANIZATION_ID AS "Subscriber Org ID"
              , (select MAX(Z.PACKAGE_ID) FROM APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING Z WHERE Z.LMA_PACKAGE_ID = A.PACKAGE_ID
               ) AS "Package ID" -- PACKAGE_ID_AA is the one used for App Analytics 
              , PACKAGE_ID AS "LMA Package ID"
              , lower(MANAGED_PACKAGE_NAMESPACE) AS "Namespace"
              , USER_TYPE AS "User Type"
              , USER_ID_TOKEN AS "User ID"
              , lower(CUSTOM_ENTITY) AS "Entity"
              , lower(CUSTOM_ENTITY_TYPE) AS "Entity Type"
              , SUM(NUM_CREATES) AS "Creates"
              , SUM(NUM_DELETES) AS "Deletes"
              , SUM(NUM_READS) AS "Reads"
              , SUM(NUM_UPDATES) AS "Updates"
              , SUM(NUM_VIEWS) "Views"
              , "Subscriber Org ID" || '-' || PACKAGE_ID AS CK
              , "Namespace"|| '-' || "Entity Type" || '-' || "Entity" "Entity_CK"
        FROM                  APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY A
        GROUP BY
                to_date("MONTH" || '-' || '01' ),
                ORGANIZATION_ID,
                PACKAGE_ID,
                MANAGED_PACKAGE_NAMESPACE,
                USER_TYPE,
                USER_ID_TOKEN ,
                CUSTOM_ENTITY,
                CUSTOM_ENTITY_TYPE              
UNION	
        SELECT  'Conga1.0' as CRM_SOURCE
              , to_date("MONTH" || '-' || '01' ) AS "DATE"
              , ORGANIZATION_ID AS "Subscriber Org ID"
              , (select MAX(Z.PACKAGE_ID) FROM APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING Z WHERE Z.LMA_PACKAGE_ID = B.PACKAGE_ID
               ) AS "Package ID" -- PACKAGE_ID_AA is the one used for App Analytics               
              , PACKAGE_ID AS "LMA Package ID"
              , lower(MANAGED_PACKAGE_NAMESPACE) AS "Namespace"
              , USER_TYPE AS "User Type"
              , USER_ID_TOKEN AS "User ID"
              , lower(CUSTOM_ENTITY) AS "Entity"
              , lower(CUSTOM_ENTITY_TYPE) AS "Entity Type"
              , SUM(NUM_CREATES) AS "Creates"
              , SUM(NUM_DELETES) AS "Deletes"
              , SUM(NUM_READS) AS "Reads"
              , SUM(NUM_UPDATES) AS "Updates"
              , SUM(NUM_VIEWS) AS "Views"
              , "Subscriber Org ID" || '-' || PACKAGE_ID AS CK
              , "Namespace"|| '-' || "Entity Type" || '-' || "Entity" "Entity_CK"
   
        FROM APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY_C1 B
        GROUP BY
                to_date("MONTH" || '-' || '01' ),
                ORGANIZATION_ID ,
                PACKAGE_ID,
                MANAGED_PACKAGE_NAMESPACE,
                USER_TYPE,
                USER_ID_TOKEN ,
                CUSTOM_ENTITY,
                CUSTOM_ENTITY_TYPE               
;