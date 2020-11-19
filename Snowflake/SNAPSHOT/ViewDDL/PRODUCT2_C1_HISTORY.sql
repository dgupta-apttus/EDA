SELECT CURRENT_ROLE();
-- CHANGE -- uncommend insert and update table name to previous snap
-- CHANGE -- manage columns near bottom
INSERT INTO APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_SNAP_1603751764227_1603920196941 
      (AVA_SFCORE__TAX_CODE__C, CANUSEREVENUESCHEDULE, CATALOG_GROUP__C, CC_REPORTS__C, CC_UNIT_PRICE__C, CREATEDBYID, CREATEDDATE, CRMC_ORG_PRODUCT_ID__C, DESCRIPTION, ENTITY__C, EXCLUDE_FROM_MRR__C, FAMILY, FY16_REVENUE_TYPE__C, HOURLY_RATE__C, HOURS__C, IA_CRM__BILLING_METHOD__C, IA_CRM__BILLING_TEMPLATE__C, IA_CRM__COST_METHOD__C, IA_CRM__DESCRIPTION_ON_SALES_TRANSACTIONS__C, IA_CRM__FLAT_FIXED_AMOUNT_FREQUENCY__C, IA_CRM__INTACCT_ENTITY_ID__C, IA_CRM__INTACCT_ENTITY__C, IA_CRM__INTACCT_SYNC_ERRORS__C, IA_CRM__INTACCT_SYNC_STATUS_IMAGE__C, IA_CRM__INTACCT_SYNC_STATUS__C, IA_CRM__ITEM_TYPE__C, IA_CRM__MRR__C, IA_CRM__SFDC_PRODUCT_ID__C, IA_CRM__STANDARD_COST__C, IA_CRM__SYNCED_WITH_INTACCT_DATE__C, IA_CRM__SYNC_WITH_INTACCT__C, IA_CRM__TAXABLE__C, IA_CRM__UNIT_OF_MEASURE__C, ID, ISACTIVE, ISDELETED, IS_PACK__C, LASTMODIFIEDBYID, LASTMODIFIEDDATE, LICENSE_TYPE__C, LINE_EDITOR_DESCRIPTION__C, LOB__C, NAME, PRODUCTCODE, PRODUCT_LINE__C, REMOVE_MONTHLY_CPQ_SCREEN__C, REQUIRES_SERVICES__C, SANDBOX_SYNC_EXTERNAL_ID__C, SERVICE_EVENT_MRR__C, SYSTEMMODSTAMP, _SDC_BATCHED_AT, _SDC_EXTRACTED_AT, _SDC_RECEIVED_AT, _SDC_SEQUENCE, _SDC_TABLE_VERSION, LASTREFERENCEDDATE, LASTVIEWEDDATE
-- add new columns as they appear
      , SNAP_LOAD_AT)
SELECT AVA_SFCORE__TAX_CODE__C, CANUSEREVENUESCHEDULE, CATALOG_GROUP__C, CC_REPORTS__C, CC_UNIT_PRICE__C, CREATEDBYID, CREATEDDATE, CRMC_ORG_PRODUCT_ID__C, DESCRIPTION, ENTITY__C, EXCLUDE_FROM_MRR__C, FAMILY, FY16_REVENUE_TYPE__C, HOURLY_RATE__C, HOURS__C, IA_CRM__BILLING_METHOD__C, IA_CRM__BILLING_TEMPLATE__C, IA_CRM__COST_METHOD__C, IA_CRM__DESCRIPTION_ON_SALES_TRANSACTIONS__C, IA_CRM__FLAT_FIXED_AMOUNT_FREQUENCY__C, IA_CRM__INTACCT_ENTITY_ID__C, IA_CRM__INTACCT_ENTITY__C, IA_CRM__INTACCT_SYNC_ERRORS__C, IA_CRM__INTACCT_SYNC_STATUS_IMAGE__C, IA_CRM__INTACCT_SYNC_STATUS__C, IA_CRM__ITEM_TYPE__C, IA_CRM__MRR__C, IA_CRM__SFDC_PRODUCT_ID__C, IA_CRM__STANDARD_COST__C, IA_CRM__SYNCED_WITH_INTACCT_DATE__C, IA_CRM__SYNC_WITH_INTACCT__C, IA_CRM__TAXABLE__C, IA_CRM__UNIT_OF_MEASURE__C, ID, ISACTIVE, ISDELETED, IS_PACK__C, LASTMODIFIEDBYID, LASTMODIFIEDDATE, LICENSE_TYPE__C, LINE_EDITOR_DESCRIPTION__C, LOB__C, NAME, PRODUCTCODE, PRODUCT_LINE__C, REMOVE_MONTHLY_CPQ_SCREEN__C, REQUIRES_SERVICES__C, SANDBOX_SYNC_EXTERNAL_ID__C, SERVICE_EVENT_MRR__C, SYSTEMMODSTAMP, _SDC_BATCHED_AT, _SDC_EXTRACTED_AT, _SDC_RECEIVED_AT, _SDC_SEQUENCE, _SDC_TABLE_VERSION, LASTREFERENCEDDATE, LASTVIEWEDDATE
-- change -- remove older NULL AS from column
-- change -- add columns from the previous snap iteration as NULL AS 
     , SNAP_LOAD_AT 
FROM APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_SNAP_1;

-- change -- check counts to insure that the recently replaced snap now has all rows
DROP TABLE APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_SNAP_1;
-- change first snap to be the recently replaced 
alter table APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_SNAP_1603751764227_1603920196941 rename to PRODUCT2_C1_SNAP_1;

--please don't -- DROP VIEW APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_HISTORY ;
-- make changes below and then rebuild the history
CREATE OR REPLACE VIEW APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_HISTORY 
COMMENT = 'Union PRODUCT2_C1 snapshots to make complete history' 
AS
WITH the_union AS (
SELECT
	AVA_SFCORE__TAX_CODE__C
	, CANUSEREVENUESCHEDULE
	, CATALOG_GROUP__C
	, CC_REPORTS__C
	, CC_UNIT_PRICE__C
	, CREATEDBYID
	, CREATEDDATE
	, CRMC_ORG_PRODUCT_ID__C
	, DESCRIPTION
	, ENTITY__C
	, EXCLUDE_FROM_MRR__C
	, FAMILY
	, FY16_REVENUE_TYPE__C
	, HOURLY_RATE__C
	, HOURS__C
	, IA_CRM__BILLING_METHOD__C
	, IA_CRM__BILLING_TEMPLATE__C
	, IA_CRM__COST_METHOD__C
	, IA_CRM__DESCRIPTION_ON_SALES_TRANSACTIONS__C
	, IA_CRM__FLAT_FIXED_AMOUNT_FREQUENCY__C
	, IA_CRM__INTACCT_ENTITY_ID__C
	, IA_CRM__INTACCT_ENTITY__C
	, IA_CRM__INTACCT_SYNC_ERRORS__C
	, IA_CRM__INTACCT_SYNC_STATUS_IMAGE__C
	, IA_CRM__INTACCT_SYNC_STATUS__C
	, IA_CRM__ITEM_TYPE__C
	, IA_CRM__MRR__C
	, IA_CRM__SFDC_PRODUCT_ID__C
	, IA_CRM__STANDARD_COST__C
	, IA_CRM__SYNCED_WITH_INTACCT_DATE__C
	, IA_CRM__SYNC_WITH_INTACCT__C
	, IA_CRM__TAXABLE__C
	, IA_CRM__UNIT_OF_MEASURE__C
	, ID
	, ISACTIVE
	, ISDELETED
	, IS_PACK__C
	, LASTMODIFIEDBYID
	, LASTMODIFIEDDATE
	, LICENSE_TYPE__C
	, LINE_EDITOR_DESCRIPTION__C
	, LOB__C
	, "NAME"
	, PRODUCTCODE
	, PRODUCT_LINE__C
	, REMOVE_MONTHLY_CPQ_SCREEN__C
	, REQUIRES_SERVICES__C
	, SANDBOX_SYNC_EXTERNAL_ID__C
	, SERVICE_EVENT_MRR__C
	, SYSTEMMODSTAMP
	, "_SDC_BATCHED_AT"
	, "_SDC_EXTRACTED_AT"
	, "_SDC_RECEIVED_AT"
	, "_SDC_SEQUENCE"
	, "_SDC_TABLE_VERSION"
	, LASTREFERENCEDDATE
	, LASTVIEWEDDATE
-- change -- add latest columns  
	, SNAP_LOAD_AT
FROM
-- change -- update to most recent snap	
	APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_SNAP_1603970756570_1603987572502
UNION
SELECT
	AVA_SFCORE__TAX_CODE__C
	, CANUSEREVENUESCHEDULE
	, CATALOG_GROUP__C
	, CC_REPORTS__C
	, CC_UNIT_PRICE__C
	, CREATEDBYID
	, CREATEDDATE
	, CRMC_ORG_PRODUCT_ID__C
	, DESCRIPTION
	, ENTITY__C
	, EXCLUDE_FROM_MRR__C
	, FAMILY
	, FY16_REVENUE_TYPE__C
	, HOURLY_RATE__C
	, HOURS__C
	, IA_CRM__BILLING_METHOD__C
	, IA_CRM__BILLING_TEMPLATE__C
	, IA_CRM__COST_METHOD__C
	, IA_CRM__DESCRIPTION_ON_SALES_TRANSACTIONS__C
	, IA_CRM__FLAT_FIXED_AMOUNT_FREQUENCY__C
	, IA_CRM__INTACCT_ENTITY_ID__C
	, IA_CRM__INTACCT_ENTITY__C
	, IA_CRM__INTACCT_SYNC_ERRORS__C
	, IA_CRM__INTACCT_SYNC_STATUS_IMAGE__C
	, IA_CRM__INTACCT_SYNC_STATUS__C
	, IA_CRM__ITEM_TYPE__C
	, IA_CRM__MRR__C
	, IA_CRM__SFDC_PRODUCT_ID__C
	, IA_CRM__STANDARD_COST__C
	, IA_CRM__SYNCED_WITH_INTACCT_DATE__C
	, IA_CRM__SYNC_WITH_INTACCT__C
	, IA_CRM__TAXABLE__C
	, IA_CRM__UNIT_OF_MEASURE__C
	, ID
	, ISACTIVE
	, ISDELETED
	, IS_PACK__C
	, LASTMODIFIEDBYID
	, LASTMODIFIEDDATE
	, LICENSE_TYPE__C
	, LINE_EDITOR_DESCRIPTION__C
	, LOB__C
	, "NAME"
	, PRODUCTCODE
	, PRODUCT_LINE__C
	, REMOVE_MONTHLY_CPQ_SCREEN__C
	, REQUIRES_SERVICES__C
	, SANDBOX_SYNC_EXTERNAL_ID__C
	, SERVICE_EVENT_MRR__C
	, SYSTEMMODSTAMP
	, "_SDC_BATCHED_AT"
	, "_SDC_EXTRACTED_AT"
	, "_SDC_RECEIVED_AT"
	, "_SDC_SEQUENCE"
	, "_SDC_TABLE_VERSION"
	, LASTREFERENCEDDATE
	, LASTVIEWEDDATE
-- change -- remove NULL AS from previous round
-- change -- ADD newest Columns as NULL AS  	
	, SNAP_LOAD_AT
FROM
	APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_SNAP_1
)
, the_unique AS (
	SELECT ID
         , _SDC_EXTRACTED_AT 
         , MAX(SNAP_LOAD_AT) AS SNAP_LOAD_AT
    FROM the_union  
    GROUP BY ID
         , _SDC_EXTRACTED_AT 
)
	SELECT A.*
	     , to_date(SYSTEMMODSTAMP) AS ACTIVITY_DATE
	     , dateadd(day, -1, to_date(A."_SDC_EXTRACTED_AT")) AS EXTRACT_DATE
	     , dateadd(day, -1, to_date(A."SNAP_LOAD_AT")) AS REPORTING_DATE	
	FROM                 the_union A
	INNER JOIN           the_unique B
	             ON  A.ID = B.ID
	             AND A._SDC_EXTRACTED_AT = B._SDC_EXTRACTED_AT
	             AND A.SNAP_LOAD_AT = B.SNAP_LOAD_AT
;	

SELECT count(*)
     , ID
     , _SDC_EXTRACTED_AT 
FROM APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_HISTORY
group by ID
       , _SDC_EXTRACTED_AT 
having count(*) > 1
;  -- 0 needs to always come out as zero

select count(distinct ID)
FROM APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_HISTORY
; --  637

select count(distinct ID, _SDC_EXTRACTED_AT)
FROM APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_HISTORY
; --  1857
