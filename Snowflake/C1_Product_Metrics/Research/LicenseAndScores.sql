
WITH tempInner as (
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
        FROM                  	APTTUS_DW.PRODUCT.MONTHLY_AA_ACTIVITY_SCORES A 
        LEFT OUTER JOIN         APTTUS_DW.PRODUCT."Master_Package_List" B
                         ON  A.PACKAGE_ID = B.LMA_PACKAGE_ID
)
select *
from tempINNER
--where PACKAGE_ID is null
;



