CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."Monthly_Score_AppAnalytics_Org"
COMMENT = 'Combine Activity Scores with License for data from App Analytics'
AS  
        SELECT    A.CRM
                , A.ORGANIZATION_ID
                , A.REPORT_YEAR
                , A.REPORT_MONTH
                , A.REPORT_DATE
                , A.LAST_ACTIVITY_MONTH
                , Coalesce(B.LMA_PACKAGE_ID, A.PACKAGE_ID) AS LMA_PACKAGE_ID
                , B.PACKAGEID AS PACKAGE_ID 
                , B.PACKAGE_NAME
                , B.PRODUCT
                , B.PRODUCTFAMILY
                , B.PRODUCTPILLAR   
                , B.PRODUCT_LINE_C1
                , A.MANAGED_PACKAGE_NAMESPACE
                , A.ACTIVITY_COUNT
                , A.ACTIVITY_P3_INTERVAL
                , A.ADOPTION_ACTIVITY_UI
                , A.ACTIVITY_DIRECTION
                , A.HISTORIC_MEDIAN_ACTIVITY_INTERVAL
                , A.HISTORIC_ACTIVITY_DIRECTION
                , A.CY_ACTIVITY
                , A.PY_ACTIVITY
                , A.YOY_ACTIVITY_INTERVAL
                , A.YOY_ACTIVITY_DIRECTION
                , A.ACTIVITY_RANGE_SCORE
                , A.UNIQUE_USERS
                , A.USER_P3_INTERVAL
                , A.ADOPTION_USER_UI
                , A.USER_DIRECTION
                , A.HISTORIC_MEDIAN_USER_INTERVAL
                , A.HISTORIC_USER_DIRECTION
                , A.CY_USERS
                , A.PY_USERS
                , A.YOY_USERS_INTERVAL
                , A.YOY_USERS_DIRECTION
                , A.USERS_RANGE_SCORE
                , A.TOTAL_MONTHS_OF_ACTIVITY
-- license info
                , C.ACTIVE_LICENSE_COUNT AS "Active License Count"
                , C.ACCOUNT_ID AS "Account ID"
                , C.ACCOUNT_NAME AS "Account Name on LMA"                
                , C.PRIMARY_LICENSE_ID AS "License ID"   
                , C.LICENSE_NAME AS "License Name"  
                , C.ACTIVE_SEAT_TYPE AS "License Seat Type"
                , C.ACTIVE_SEATS AS "Seats Active"
                , C.ACTIVE_USED AS "Used Active Seats"   
                , CASE
                    WHEN C.ACTIVE_SEAT_TYPE = 'Site'
                      THEN NULL
                    WHEN C.ACTIVE_SEAT_TYPE IS NULL
                      THEN NULL  
                    WHEN C.ACTIVE_SEAT_TYPE = 'Seats'
                     AND C.ACTIVE_SEATS > 0
                      THEN ((C.ACTIVE_USED/C.ACTIVE_SEATS)*100)::INTEGER
                   ELSE 0
                  END AS "Assigned Ratio"                 
                , CASE
                    WHEN C.ACTIVE_SEAT_TYPE = 'Seats'  
                      AND C.ACTIVE_USED > 0
                        THEN ((COALESCE(A.UNIQUE_USERS, 0)/C.ACTIVE_USED)*100)::INTEGER
                   ELSE NULL
                  END AS "Usage Ratio" 
                , CASE
                    WHEN C.ACTIVE_SEAT_TYPE <> 'Seats'
                      THEN NULL
                    WHEN C.ACTIVE_SEAT_TYPE IS NULL
                      THEN NULL  
                    WHEN  C.ACTIVE_SEATS > 0
                     AND C.CRM_SOURCE = 'Conga1.0'
                      THEN ((COALESCE(A.UNIQUE_USERS, 0)/C.ACTIVE_SEATS)*100)::INTEGER
                   ELSE 0
                  END AS "Usage/Purchased Ratio"                              
        FROM                  	APTTUS_DW.PRODUCT.MONTHLY_AA_ACTIVITY_SCORES A 
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT."Master_Package_List" B
                         ON  A.PACKAGE_ID = B.LMA_PACKAGE_ID
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY C
                         ON   A.ORGANIZATION_ID = C.CUSTOMER_ORG_15 
                         AND  B.PACKAGEID = C.PACKAGE_ID
                         AND  A.REPORT_DATE = C.REPORTING_DATE                                
;



