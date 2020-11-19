-- APTTUS_DW.SF_PRODUCTION.PRODUCT_LICENSE source 
CREATE OR replace VIEW product_license comment = 'Product Usage in support of MAU\DAU joined with Account, Lead and License     ---V1 07/30/2020 ---Naveen ---V2 08/12/2020 ---Naveen - Adding Package Version realtionship ---V3 08/20/2020 ---Naveen - Added SFLMA__PACKAGE_VERSION__C * Account ID to join with Package Version ---V4 08/21/2020 ---Naveen - Cleaned up fileds and names ---V5 08/24/2020 ---Naveen - Added Expiration and custom last modified dates ---V6 09/01/2020 ---Naveen - Added Composite Key 1 ' AS 
                       WITH ctelicense                                                                                                                                                                                                        AS
                       ( 
  SELECT l."NAME" "License Name", 
 l.sflma__subscriber_org_id__c "Subscriber Org ID" , 
 l.sflma__used_licenses__c "Used Licenses", 
 l.sflma__licensed_seats__c "Licensed Seats", 
 l.sflma__seats__c "Seats", 
 l.sflma__license_status__c "License Status" , 
 l.sflma__package__c "Package ID", 
 l.sflma__account__c "Account ID", 
 l.sflma__package_version__c "Package Version ID", 
 l.sflma__expiration_date__c "Expiration Date" , 
 l.custom_last_modified_date__c "Custom Last Modified Date",
 "Subscriber Org ID" ||'-' || "Package ID" ck1 
  FROM   apttus_dw.salesforce_clmcpq.sflma__license__c l --1458929 
  WHERE  l.sflma__subscriber_org_id__c IS NOT NULL 
  AND    l.sflma__org_status__c ='ACTIVE' 
 --L.SFLMA__SUBSCRIBER_ORG_ID__C = '00DG0000000kydc' 
 --AND L."NAME" ='L-1358717' 
                       ) 
                SELECT l.* 
                FROM   ctelicense l;