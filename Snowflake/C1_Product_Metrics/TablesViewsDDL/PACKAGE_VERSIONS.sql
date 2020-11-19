--DROP VIEW APTTUS_DW.PRODUCT."Package_Versions" ; 

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."Package_Versions" 
COMMENT = 'Package versions from both SF Instaces CPQ\CLM and Conga1
' 
AS
        SELECT  'Apttus1.0' as "CRM"
                , Id AS "Package Version ID" 
                , IsDeleted AS "Deleted" 
                , Name AS "Package Version Name" 
                , SystemModstamp AS "System Modstamp" 
                , sfLma__Package__c AS "LMA Package ID" 
                , sfLma__Version__c AS "Version "
                , NULL AS "Version Detail"  
                , Package_ID__c AS "Package ID" 
                , Release_Name__c AS "Release Name" 
                , Release_Patch_Name__c AS "Release Patch Name" 
        FROM 
        APTTUS_DW.SALESFORCE_CLMCPQ.SFLMA__PACKAGE_VERSION__C
        WHERE ISDELETED = 'False'
UNION ALL
        SELECT  'Conga1.0' as "CRM" 
                , Id AS "Package Version ID"  
                , IsDeleted AS "Deleted" 
                , Name AS "Package Version Name" 
                , SystemModstamp AS "System Modstamp" 
                , sfLma__Package__c AS "LMA Package ID" 
                , sfLma__Version__c AS "Version" 
                , SFLMA__VERSION_NUMBER__C AS "Version Detail"
                , NULL AS "Package ID" 
                , NULL AS "Release Name" 
                , NULL AS "Release Patch Name" 
        FROM 
        APTTUS_DW.SF_CONGA1_1.SFLMA__PACKAGE_VERSION__C
        WHERE ISDELETED = 'False'        
;
 