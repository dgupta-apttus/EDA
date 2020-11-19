WITH joinAndCase AS ( 
	SELECT A.ACCOUNTID__C
	     , A.SALESFORCE_ACCOUNT_ID__C AS CUSTOMER_ORG_18
	     , A.SALESFORCE_ORG_ID_15__C AS ORG_15
	     , A.COMPOSER_LICENSE__C
	     , A.LICENSED_SEATS__C
	     , A.CONGA_LICENSES__C
	     , B.PACKAGE_ID 
	     , B.PACKAGE_NAME
	     , B.LICENSE_SEAT_TYPE
	     , B.SEATS
	     , B.LICENSE_ID
	     , B.LICENSE_NAME
	     , CASE 
	         WHEN A.LICENSED_SEATS__C = -1 
	          AND A.CONGA_LICENSES__C > 0 -- both SITE and Seat count 
	            THEN 1::BOOLEAN
	         WHEN A.LICENSED_SEATS__C > -1 
	          AND A.LICENSED_SEATS__C <> A.CONGA_LICENSES__C -- seat counts don't agree
	            THEN 1::BOOLEAN
	         WHEN B.LICENSE_ID IS NULL 
	            THEN 1::BOOLEAN
	         WHEN A.LICENSED_SEATS__C = -1 
	          AND B.LICENSE_SEAT_TYPE = 'Seats'
	            THEN 1::BOOLEAN
	         WHEN A.LICENSED_SEATS__C > -1 AND B.LICENSE_SEAT_TYPE = 'Site'
	            THEN 1::BOOLEAN
	         WHEN A.LICENSED_SEATS__C > 0
	          AND A.LICENSED_SEATS__C <> B.SEATS  
	            THEN 1::BOOLEAN
	       ELSE 0::BOOLEAN
	      END AS LICENSE_MISMATCH 
	     , CASE 
	         WHEN A.LICENSED_SEATS__C = -1 
	          AND A.CONGA_LICENSES__C > 0 -- both SITE and Seat count 
	            THEN 'ORGs indicates both Site and Seats'
	         WHEN A.LICENSED_SEATS__C > -1 
	          AND A.LICENSED_SEATS__C <> A.CONGA_LICENSES__C -- seat counts don't agree
	            THEN 'ORGs seat counts do not agree'
	         WHEN B.LICENSE_ID IS NULL 
	            THEN 'LMA License record not found'
	         WHEN A.LICENSED_SEATS__C = -1 
	          AND B.LICENSE_SEAT_TYPE = 'Seats'
	            THEN 'ORGs and LMA do not agree on License Type'
	         WHEN A.LICENSED_SEATS__C > -1 
	          AND B.LICENSE_SEAT_TYPE = 'Site'
	            THEN 'ORGs and LMA do not agree on License Type'
	         WHEN A.LICENSED_SEATS__C > 0
	          AND A.LICENSED_SEATS__C <> B.SEATS  
	            THEN 'ORGs and LMA do not agree on Seat Counts'
	       ELSE 'OK'
	      END AS MISMATCH_DESCRIPTION 	      
	FROM                         APTTUS_DW.SF_CONGA1_0.SALESFORCE_ORG__C A
	LEFT OUTER JOIN              APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT B         
	                    ON   A.SALESFORCE_ACCOUNT_ID__C = B.CUSTOMER_ORG
	                    AND  A.COMPOSER_LICENSE__C = B.LICENSE_ID
	                    AND  B.PRODUCT = 'Conga Composer'
	WHERE A.ORG_TYPE__C = 'Production'
	  AND A.COMPOSER_LICENSE__C is not null
)
, selectErrors as (
        SELECT * 
        FROM joinAndCase
        WHERE LICENSE_MISMATCH = TRUE
)        
        SELECT COUNT(*)
             , MISMATCH_DESCRIPTION
        FROM selectErrors
        group by MISMATCH_DESCRIPTION      
;