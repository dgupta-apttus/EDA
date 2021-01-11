-- APTTUS_DW.SF_PRODUCTION.PRODUCT_APPANALYTICS_SUMMARY source

CREATE VIEW APTTUS_DW.PRODUCT.PRODUCT_APPANALYTICS_SUMMARY 
COMMENT = 'Product App Analytics Summary Data from a1 and c1
' 
AS;
        SELECT
                to_date(A."MONTH" || '-' || '01' ) "DATE",
                B.ACCOUNT_ID AS "Account ID",
                ORGANIZATION_ID "Subscriber Org ID",
                A.PACKAGE_ID "Package ID",
                lower(MANAGED_PACKAGE_NAMESPACE) "Namespace",
                USER_TYPE "User Type",
                USER_ID_TOKEN "User ID",
                lower(CUSTOM_ENTITY) "Entity",
                lower(CUSTOM_ENTITY_TYPE) "Entity Type",
                SUM(NUM_CREATES) "Creates",
                SUM(NUM_DELETES) "Deletes",
                SUM(NUM_READS) "Reads",
                SUM(NUM_UPDATES) "Updates",
                SUM(NUM_VIEWS) "Views",
                "Subscriber Org ID" || '-' || "Package ID" CK,
                "Namespace"|| '-' || "Entity Type" || '-' || "Entity" "Entity_CK"
        FROM                  APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY A
        LEFT OUTER JOIN       APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY B 
                          ON    A.ORGANIZATION_ID = B.CUSTOMER_ORG_15
                          AND A.PACKAGE_ID = B.PACKAGE_ID
                          AND to_date(A."MONTH" || '-' || '01' ) = B.REPORTING_DATE  
        GROUP BY
                to_date(A."MONTH" || '-' || '01' ),
                B.ACCOUNT_ID, 
                ORGANIZATION_ID,
                A.PACKAGE_ID,
                MANAGED_PACKAGE_NAMESPACE,
                USER_TYPE,
                USER_ID_TOKEN ,
                CUSTOM_ENTITY,
                CUSTOM_ENTITY_TYPE
;                
UNION	
        SELECT
                to_date(S."MONTH" || '-' || '01' ) "DATE",
                ORGANIZATION_ID "Subscriber Org ID",
                PACKAGE_ID "Package ID",
                lower(MANAGED_PACKAGE_NAMESPACE) "Namespace",
                USER_TYPE "User Type",
                USER_ID_TOKEN "User ID",
                lower(CUSTOM_ENTITY) "Entity",
                lower(CUSTOM_ENTITY_TYPE) "Entity Type",
                SUM(NUM_CREATES) "Creates",
                SUM(NUM_DELETES) "Deletes",
                SUM(NUM_READS) "Reads",
                SUM(NUM_UPDATES) "Updates",
                SUM(NUM_VIEWS) "Views",
                "Subscriber Org ID" || '-' || "Package ID" CK,
                "Namespace"|| '-' || "Entity Type" || '-' || "Entity" "Entity_CK"
        FROM APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY_C1 S
        GROUP BY
                to_date(S."MONTH" || '-' || '01' ),
                ORGANIZATION_ID ,
                PACKAGE_ID,
                MANAGED_PACKAGE_NAMESPACE,
                USER_TYPE,
                USER_ID_TOKEN ,
                CUSTOM_ENTITY,
                CUSTOM_ENTITY_TYPE               
;	

select * from (
        SELECT
                to_date(A."MONTH" || '-' || '01' )                   AS "DATE"
              , B.ACCOUNT_ID                                         AS "Account ID"
              , ORGANIZATION_ID                                      AS "Subscriber Org ID"
              , A.PACKAGE_ID                                         AS "LMA Package ID"
              , (select MAX(PACKAGEID) FROM APTTUS_DW.PRODUCT."Master_Package_List" 
                 WHERE LMA_PACKAGE_ID = A.PACKAGE_ID
                )                                                    AS PACKAGE_ID     
              , B.PRIMARY_LICENSE_ID                                 AS "License ID"   
              , B.LICENSE_NAME                                       AS "License Name"   
              , B.ACTIVE_SEAT_TYPE                                   AS "License Seat Type"                   
              , lower(MANAGED_PACKAGE_NAMESPACE)                     AS "Namespace"
              , USER_TYPE                                            AS "User Type"
              , USER_ID_TOKEN                                        AS "User ID"
              , lower(CUSTOM_ENTITY)                                 AS "Entity"
              , lower(CUSTOM_ENTITY_TYPE)                            AS "Entity Type"
              , SUM(NUM_CREATES)                                     AS "Creates"
              , SUM(NUM_DELETES)                                     AS "Deletes"
              , SUM(NUM_READS)                                       AS "Reads"
              , SUM(NUM_UPDATES)                                     AS "Updates"
              , SUM(NUM_VIEWS)                                       AS "Views"
              , ORGANIZATION_ID || '-' || A.PACKAGE_ID               AS CK
              , "Namespace"|| '-' || "Entity Type" || '-' || "Entity"  AS "Entity_CK"
        FROM                  APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY A
        LEFT OUTER JOIN       APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY B 
                          ON    A.ORGANIZATION_ID = B.CUSTOMER_ORG_15
                          --AND A.PACKAGE_ID = B.PACKAGE_ID
                          --AND to_date(A."MONTH" || '-' || '01' ) = B.REPORTING_DATE  
        GROUP BY
                 to_date(A."MONTH" || '-' || '01' )
              ,  B.ACCOUNT_ID
              ,  ORGANIZATION_ID
              ,  A.PACKAGE_ID
              ,  B.PRIMARY_LICENSE_ID   
              ,  B.LICENSE_NAME    
              ,  B.ACTIVE_SEAT_TYPE 
              ,  MANAGED_PACKAGE_NAMESPACE
              ,  USER_TYPE
              ,  USER_ID_TOKEN 
              ,  CUSTOM_ENTITY
              ,  CUSTOM_ENTITY_TYPE
)                
where "Account ID" is not null
;

with convert_package as (;
        SELECT  
                to_date("MONTH" || '-' || '01' ) AS ACTIVITY_MONTH_DATE
               , ORGANIZATION_ID 
               , 'Apttus1.0' as CRM_SOURCE
               , PACKAGE_ID AS LMA_PACKAGE_ID 
               , (select MAX(PACKAGEID) FROM APTTUS_DW.PRODUCT."Master_Package_List" WHERE LMA_PACKAGE_ID = PACKAGE_ID
               ) AS PACKAGE_ID  
               , MANAGED_PACKAGE_NAMESPACE  
               , USER_TYPE  
               , USER_ID_TOKEN  
               , CUSTOM_ENTITY  
               , CUSTOM_ENTITY_TYPE  
               , NUM_CREATES
               , NUM_DELETES  
               , NUM_READS  
               , NUM_UPDATES  
               , NUM_VIEWS 
        FROM                    APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY
UNION
        SELECT        
                to_date("MONTH" || '-' || '01' ) AS ACTIVITY_MONTH_DATE
               , ORGANIZATION_ID 
               , 'Conga1.0' as CRM_SOURCE
               , PACKAGE_ID AS LMA_PACKAGE_ID 
               , (select MAX(PACKAGEID) FROM APTTUS_DW.PRODUCT."Master_Package_List" WHERE LMA_PACKAGE_ID = PACKAGE_ID
               ) AS PACKAGE_ID  
               , MANAGED_PACKAGE_NAMESPACE  
               , USER_TYPE  
               , USER_ID_TOKEN  
               , CUSTOM_ENTITY  
               , CUSTOM_ENTITY_TYPE  
               , NUM_CREATES
               , NUM_DELETES  
               , NUM_READS  
               , NUM_UPDATES  
               , NUM_VIEWS 
        FROM                    APTTUS_DW.SF_PRODUCTION.PRODUCTMETRICSDATA_SUMMARY_C1        
order by 8
;

)
        select 
               A.ORGANIZATION_ID AS CUSTOMER_ORG
             , A.CRM_SOURCE
             , A.LMA_PACKAGE_ID
             , A.ACTIVITY_MONTH_DATE
             , A.USER_ID_TOKEN   
             , A.PACKAGE_ID           
             , B.ACCOUNT_ID
        FROM                    convert_package A
        INNER JOIN              
        --LEFT OUTER JOIN
                                APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY B 
                           ON  A.ORGANIZATION_ID = B.CUSTOMER_ORG    
                           AND A.PACKAGE_ID = B.PACKAGE_ID 
        WHERE A.CRM_SOURCE = 'Conga1.0'                     
;                    

SELECT *
FROM APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY
WHERE CUSTOMER_ORG = '00D20000000oFs1'
;