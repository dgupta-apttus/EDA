CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.APP_ANALYTICS_PACKAGE_SUMMARY
COMMENT = 'build out app analytics summary joined for C1 and A1 Summarized at Org and Package
'
AS
	SELECT CRM_SOURCE AS CRM 
	     , "DATE" As REPORT_DATE
	     , "Subscriber Org ID" AS ORGANIZATION_ID
	     , "Package ID" AS PACKAGE_ID
	     , "LMA Package ID" AS LMA_PACKAGE_ID
	     , "Namespace" AS MANAGED_PACKAGE_NAMESPACE
	     , COALESCE(COUNT(DISTINCT "User ID"), 0) AS MONTHLY_ACTIVE_USERS
	     , COALESCE(SUM("Creates"), 0) AS NUM_CREATES
	     , COALESCE(SUM("Deletes"), 0) AS NUM_DELETES
	     , COALESCE(SUM("Reads"), 0) AS NUM_READS
	     , COALESCE(SUM("Updates"), 0) AS NUM_UPDATES
	     , COALESCE(SUM("Views"), 0) AS NUM_VIEWS 
             , (NUM_READS + NUM_ViEWS) AS ACCESS_ACTIVITY
             , (NUM_CREATES + NUM_UPDATES + NUM_DELETES) AS MANIPULATION_ACTIVITY
             , (NUM_READS + NUM_VIEWS + NUM_CREATES + NUM_UPDATES + NUM_DELETES) AS MONTHLY_ACTIVITY	      
	FROM APTTUS_DW.PRODUCT.APPANALYTICS_SUMMARY
	GROUP BY CRM_SOURCE  
	     , "DATE" 
	     , "Subscriber Org ID" 
	     , "Package ID" 
	     , "LMA Package ID" 
	     , "Namespace";
	 
	 
	 

	