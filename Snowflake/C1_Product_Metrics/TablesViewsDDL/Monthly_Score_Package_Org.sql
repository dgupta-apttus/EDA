
CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."Monthly_Score_Package_Org"
COMMENT = 'Combine Activity Scores with License for data from App Analytics
-- 2020/12/16  using new standards for product to line and family roll up -- gdw
'
AS 
SELECT 	CRM
	, DATA_SOURCE
	, ORG_SOURCE
	, ORGANIZATION_ID
	, REPORT_YEAR
	, REPORT_MONTH
	, REPORT_DATE
	, LAST_ACTIVITY_MONTH
	, LMA_PACKAGE_ID
	, PACKAGE_ID
	, PACKAGE_NAME
	, PRODUCT
	, PRODUCT_LINE
	, PRODUCT_FAMILY
	, PRODUCT_LINE_C1
	, MANAGED_PACKAGE_NAMESPACE
	, ACTIVITY_COUNT
	, ACTIVITY_P3_INTERVAL
	, ADOPTION_ACTIVITY_UI
	, ACTIVITY_DIRECTION
	, HISTORIC_MEDIAN_ACTIVITY_INTERVAL
	, HISTORIC_ACTIVITY_DIRECTION
	, CY_ACTIVITY
	, PY_ACTIVITY
	, YOY_ACTIVITY_INTERVAL
	, YOY_ACTIVITY_DIRECTION
	, ACTIVITY_RANGE_SCORE
	, UNIQUE_USERS
	, USER_P3_INTERVAL
	, ADOPTION_USER_UI
	, USER_DIRECTION
	, HISTORIC_MEDIAN_USER_INTERVAL
	, HISTORIC_USER_DIRECTION
	, CY_USERS
	, PY_USERS
	, YOY_USERS_INTERVAL
	, YOY_USERS_DIRECTION
	, USERS_RANGE_SCORE
	, TOTAL_MONTHS_OF_ACTIVITY
	, "Active License Count"
	, "Account ID"
	, "Account Name on LMA"
	, "License ID"
	, "License Name"
	, "License Seat Type"
	, "Status - Sandbox"
	, "Status - License"
	, "Status - Org"
	, "Seats Active"
	, "Used Active Seats"
	, "Seats Non-Prod"
	, "Seats Sandbox"
	, "Expiration Date"
	, "Expiration Text"
	, "Install Date"
	, "Install Text"
	, "Uninstall Date"
	, LIC_ASSIGNED_RAW
	, LIC_ASSIGNED_UI
	, "Assigned Ratio"
	, LIC_USAGE_RAW
	, LIC_USAGE_UI
	, "Usage Ratio"
	, USAGE_EXCEEDS_SEATS
	, LIC_USEPUR_RAW
	, LIC_USEPUR_UI
	, "Usage/Purchased Ratio"
	, "Months Since Last User"
	, LAST_USER_TIME_UI
	, ADOPTION_V1
	, "Service Events Percentage"
FROM APTTUS_DW.PRODUCT."Monthly_Score_AppAnalytics_Org"
WHERE CRM = 'Apttus1.0'
UNION
SELECT 	CRM
	, DATA_SOURCE
	, ORG_SOURCE
	, ORGANIZATION_ID
	, REPORT_YEAR
	, REPORT_MONTH
	, REPORT_DATE
	, LAST_ACTIVITY_MONTH
	, LMA_PACKAGE_ID
	, PACKAGE_ID
	, PACKAGE_NAME
	, PRODUCT
	, PRODUCT_LINE
	, PRODUCT_FAMILY
	, PRODUCT_LINE_C1
	, MANAGED_PACKAGE_NAMESPACE
	, ACTIVITY_COUNT
	, ACTIVITY_P3_INTERVAL
	, ADOPTION_ACTIVITY_UI
	, ACTIVITY_DIRECTION
	, HISTORIC_MEDIAN_ACTIVITY_INTERVAL
	, HISTORIC_ACTIVITY_DIRECTION
	, CY_ACTIVITY
	, PY_ACTIVITY
	, YOY_ACTIVITY_INTERVAL
	, YOY_ACTIVITY_DIRECTION
	, ACTIVITY_RANGE_SCORE
	, UNIQUE_USERS
	, USER_P3_INTERVAL
	, ADOPTION_USER_UI
	, USER_DIRECTION
	, HISTORIC_MEDIAN_USER_INTERVAL
	, HISTORIC_USER_DIRECTION
	, CY_USERS
	, PY_USERS
	, YOY_USERS_INTERVAL
	, YOY_USERS_DIRECTION
	, USERS_RANGE_SCORE
	, TOTAL_MONTHS_OF_ACTIVITY
	, "Active License Count"
	, "Account ID"
	, "Account Name on LMA"
	, "License ID"
	, "License Name"
	, "License Seat Type"
	, "Status - Sandbox"
	, "Status - License"
	, "Status - Org"
	, "Seats Active"
	, "Used Active Seats"
	, "Seats Non-Prod"
	, "Seats Sandbox"
	, "Expiration Date"
	, "Expiration Text"
	, "Install Date"
	, "Install Text"
	, "Uninstall Date"
	, LIC_ASSIGNED_RAW
	, LIC_ASSIGNED_UI
	, "Assigned Ratio"
	, LIC_USAGE_RAW
	, LIC_USAGE_UI
	, "Usage Ratio"
	, USAGE_EXCEEDS_SEATS
	, LIC_USEPUR_RAW
	, LIC_USEPUR_UI
	, "Usage/Purchased Ratio"
	, "Months Since Last User"
	, LAST_USER_TIME_UI
	, ADOPTION_V1
	, "Service Events Percentage" 
FROM APTTUS_DW.PRODUCT."Monthly_Score_Pipeline_Org"
;
