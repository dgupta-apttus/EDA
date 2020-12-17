
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."Monthly_Score_Pipeline_Org"
COMMENT = 'Combine Activity Scores with License for data from App Analytics
-- 2020/12/16  replace OLD> Master_Package_List with NEW> MASTER_PRODUCT_PACKAGE_MAPPING for product line lookups -- gdw
'
AS 
        SELECT    'Conga1.0' AS CRM
                , 'Pipeline' AS DATA_SOURCE 
                , A.ORG_SOURCE
                , A.SOURCE_ORG_ID AS ORGANIZATION_ID               
                , A.REPORT_YEAR
                , A.REPORT_MONTH
                , A.REPORT_DATE
                , A.LAST_ACTIVITY_MONTH
                , B.LMA_PACKAGE_ID
                , A.PACKAGE_ID 
                , B.PACKAGE_NAME
                , B.PRODUCT
                , B.PRODUCT_LINE
                , B.PRODUCT_FAMILY  
                , A.PRODUCT_LINE AS PRODUCT_LINE_C1
                , A.PACKAGE_NAMESPACE AS MANAGED_PACKAGE_NAMESPACE
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
                , COALESCE(C.ACTIVE_LICENSE_COUNT, 0) AS "Active License Count"
                , C.ACCOUNT_ID AS "Account ID"
                , C.ACCOUNT_NAME AS "Account Name on LMA"                
                , C.PRIMARY_LICENSE_ID AS "License ID"   
                , C.LICENSE_NAME AS "License Name"  
                , COALESCE(C.ACTIVE_SEAT_TYPE, 'Unknown') AS "License Seat Type"
                , C.IS_SANDBOX AS "Status - Sandbox"
                , C.STATUS AS "Status - License"
                , C.ORG_STATUS AS "Status - Org"    
                , C.ACTIVE_SEATS AS "Seats Active"
                , C.ACTIVE_USED AS "Used Active Seats" 
                , C.NONPROD_SEATS AS "Seats Non-Prod"
                , C.SANDBOX_SEATS AS "Seats Sandbox"                   
                , C.EXPIRATION_DATE AS "Expiration Date"
                , C.EXPIRATION_DATE_STRING AS "Expiration Text"
                , TO_DATE(C.INSTALL_DATE) AS "Install Date"
                , C.INSTALL_DATE_STRING AS "Install Text"                
                , C.UNINSTALL_DATE AS "Uninstall Date"                          
                , CASE
                    WHEN C.ACTIVE_SEAT_TYPE = 'Seats'
                     AND C.ACTIVE_SEATS > 0
                      THEN C.ACTIVE_USED/C.ACTIVE_SEATS
                   ELSE 0
                  END AS LIC_ASSIGNED_RAW
                , LEAST(1, LIC_ASSIGNED_RAW) AS LIC_ASSIGNED_UI                          
                , ROUND(LIC_ASSIGNED_RAW*100) AS "Assigned Ratio"                 
                , CASE
                    WHEN C.ACTIVE_SEAT_TYPE = 'Seats'  
                      AND C.ACTIVE_USED > 0
                        THEN A.UNIQUE_USERS/C.ACTIVE_USED
                   ELSE 0
                  END AS LIC_USAGE_RAW
                , LEAST(1, LIC_USAGE_RAW) AS LIC_USAGE_UI
                , ROUND(LIC_USAGE_RAW*100) as "Usage Ratio"        
                , CASE 
                    WHEN C.ACTIVE_SEAT_TYPE = 'Seats'   
                       AND C.ACTIVE_USED < A.UNIQUE_USERS  
                      THEN 1::BOOLEAN
                   ELSE 0::BOOLEAN
                  END AS USAGE_EXCEEDS_SEATS    
                , CASE 
                    WHEN C.ACTIVE_SEAT_TYPE = 'Seats' 
                      AND C.ACTIVE_SEATS > 0
                        THEN A.UNIQUE_USERS/C.ACTIVE_SEATS
                   ELSE 0
                  END AS LIC_USEPUR_RAW
                , LEAST(1, LIC_USEPUR_RAW) AS LIC_USEPUR_UI
                , ROUND(LIC_USEPUR_RAW*100) AS "Usage/Purchased Ratio"                            
                , DATEDIFF(month, A.LAST_ACTIVITY_MONTH, A.REPORT_DATE) AS "Months Since Last User" 
                , (10 - "Months Since Last User")/10 as LAST_USER_TIME_UI
                , CASE
                    WHEN C.ACTIVE_SEAT_TYPE = 'Seats'
                      THEN ROUND(((LIC_USAGE_UI * .35) + (A.ADOPTION_ACTIVITY_UI * .4) + (LIC_ASSIGNED_UI * .25)) * 100, 2)   
                   ELSE ROUND(((A.ADOPTION_ACTIVITY_UI * .4) + (A.ADOPTION_USER_UI * .35) + (LAST_USER_TIME_UI * .25)) * 100, 2)
                  END AS ADOPTION_V1
                , A.PERCENT_SERVICE_EVENTS::INTEGER AS "Service Events Percentage"                          
        FROM                  	APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY_SCORES A 
        LEFT OUTER JOIN         APTTUS_DW.SF_PRODUCTION.MASTER_PRODUCT_PACKAGE_MAPPING B
                         ON  A.PACKAGE_ID = B.PACKAGE_ID
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY C
                         ON   A.SOURCE_ORG_ID = C.CUSTOMER_ORG_18
                         AND  A.PACKAGE_ID = C.PACKAGE_ID
                         AND  A.REPORT_DATE = C.REPORTING_DATE                                
;


select count(*) 
      , REPORT_DATE
      , CRM
FROM APTTUS_DW.PRODUCT."Monthly_Score_Pipeline_Org"
group by REPORT_DATE
      , CRM
order by 2 desc, 3 
;
/*
--testing
select * 
FROM APTTUS_DW.PRODUCT."Monthly_Score_Pipeline_Org"
where "License ID" is null

;
select count(*), REPORT_DATE, PRODUCT_LINE
from APTTUS_DW.PRODUCT.MONTHLY_ACTIVITY_SCORES
WHERE REPORT_DATE > '2020-07-01'
GROUP BY REPORT_DATE, PRODUCT_LINE
ORDER BY PRODUCT_LINE
;

select count(*) 
      , REPORT_DATE
      , CRM
      , PRODUCT
      , PACKAGE_ID
      , PRODUCT_LINE_C1 
FROM APTTUS_DW.PRODUCT."Monthly_Score_Pipeline_Org"
group by REPORT_DATE
      , CRM
      , PRODUCT
      , PACKAGE_ID
      , PRODUCT_LINE_C1 
order by PRODUCT_LINE_C1 
       , REPORT_DATE desc        
;


select ACTIVE_SEAT_TYPE, REPORTING_DATE, COUNT(*)
from APTTUS_DW.PRODUCT.LMA_LIC_PACKAGE_MONTHLY
group by ACTIVE_SEAT_TYPE, REPORTING_DATE
order by 1, 2
;
*/