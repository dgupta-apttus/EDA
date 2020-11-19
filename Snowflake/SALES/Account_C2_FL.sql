DROP VIEW APTTUS_DW.SF_PRODUCTION."Account_C2_FL";

CREATE OR REPLACE VIEW APTTUS_DW.SF_PRODUCTION."Account_C2_FL"
COMMENT = 'Skin on Field Labels (FL) for a few chosen column, feel free to add more
created 10/13/20 Greg
'
AS 
SELECT
	  CASE 
	    WHEN "SOURCE" = 'CONGA1.0' 
	      THEN 'Conga1.0'
	   ELSE "SOURCE"   
	  END AS "CRM Source"
	, ACCOUNTID_18__C AS "Account ID"
	, ACCOUNT_NAME AS "Account Name"
	, "TYPE" AS "Account Type"
	, GEO_NAME AS "Geo"
	, REGION_NAME AS "Region"
	, SEGMENT_NAME AS "Segment"
	, ESTABLISHING_PARTNER_NAME AS "Establishing Partner"
	, TO_DATE(RENEWAL_DATE) AS "Next Renewal" 
	, TO_DATE(CUSTOMER_SINCE_DATE) AS "Customer Since" 
	, INDUSTRY AS "Industry"
	, DNB_SIC4_CODE1 AS "SIC4"
	, NETSUITE_ID AS "Netsuite ID" 
FROM
	APTTUS_DW.SF_PRODUCTION."Account_C2"
WHERE TEST_ACCOUNT_C1 = false 
;

