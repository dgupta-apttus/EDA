SELECT CURRENT_ROLE();

Select TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME, MAX(ORDINAL_POSITION)
 FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME LIKE 'CURRENCYTYPE_SNAP%'
 GROUP by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
 order by 1
; 

with get_last as(
        Select MAX(ORDINAL_POSITION) as LAST_COL
         FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
         WHERE TABLE_NAME = 'CURRENCYTYPE_SNAP_1'
)
Select TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME
 FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME LIKE 'CURRENCYTYPE_SNAP%'
   AND ORDINAL_POSITION > ((SELECT LAST_COL from get_last)-1)
   AND COLUMN_NAME <> 'SNAP_LOAD_AT'
;  

Select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME LIKE 'CURRENCYTYPE_SNAP%'
ORDER BY ORDINAL_POSITION, TABLE_NAME 
;

select count(*) from APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1;
select count(*) from APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1604727252490_1606322561365;
select count(*) from APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1604727252490_1606322561365;


-- CHANGE -- uncommend insert and update table name to previous snap
-- CHANGE -- manage columns near bottom
INSERT INTO APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_next_next
     ( CONVERSIONRATE, CREATEDBYID, CREATEDDATE, DECIMALPLACES, ID, ISACTIVE, ISCORPORATE, ISOCODE
     , LASTMODIFIEDBYID, LASTMODIFIEDDATE, SYSTEMMODSTAMP
     , _SDC_BATCHED_AT, _SDC_EXTRACTED_AT, _SDC_RECEIVED_AT, _SDC_SEQUENCE, _SDC_TABLE_VERSION 
-- add new colunmns     
     , SNAP_LOAD_AT) 
SELECT 
       CONVERSIONRATE, CREATEDBYID, CREATEDDATE, DECIMALPLACES, ID, ISACTIVE, ISCORPORATE, ISOCODE
     , LASTMODIFIEDBYID, LASTMODIFIEDDATE, SYSTEMMODSTAMP
     , _SDC_BATCHED_AT, _SDC_EXTRACTED_AT, _SDC_RECEIVED_AT, _SDC_SEQUENCE, _SDC_TABLE_VERSION 
-- change -- remove older NULL AS from column
-- change -- add columns from the previous snap iteration as NULL AS 
     , SNAP_LOAD_AT 
FROM APTTUS_DW.SNAPSHOTS.APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1;

-- change -- check counts to insure that the recently replaced snap now has all rows
D*ROP TABLE APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1;
-- change first snap to be the recently replaced 
a*lter table APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1604728034695_1606319140742 rename to APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1;
select count(*) from APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1;

-- make changes below and then rebuild the history
CREATE OR REPLACE VIEW APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_HISTORY  
COMMENT = 'Union CURRENCYTYPE snapshots to make complete history'
AS 
WITH the_union AS (
SELECT
       CONVERSIONRATE, CREATEDBYID, CREATEDDATE, DECIMALPLACES, ID, ISACTIVE, ISCORPORATE, ISOCODE
     , LASTMODIFIEDBYID, LASTMODIFIEDDATE, SYSTEMMODSTAMP
     , "_SDC_BATCHED_AT", "_SDC_EXTRACTED_AT", "_SDC_RECEIVED_AT", "_SDC_SEQUENCE", "_SDC_TABLE_VERSION"
-- change -- add latest columns  
	, SNAP_LOAD_AT
FROM
-- change -- update to most recent snap	
        APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1604727252490_1606322561365
/* add union when it arrives
UNION
SELECT
       CONVERSIONRATE, CREATEDBYID, CREATEDDATE, DECIMALPLACES, ID, ISACTIVE, ISCORPORATE, ISOCODE
     , LASTMODIFIEDBYID, LASTMODIFIEDDATE, SYSTEMMODSTAMP
     , "_SDC_BATCHED_AT", "_SDC_EXTRACTED_AT", "_SDC_RECEIVED_AT", "_SDC_SEQUENCE", "_SDC_TABLE_VERSION"
-- change -- remove NULL AS from previous round
-- change -- ADD newest Columns as NULL AS  
	, SNAP_LOAD_AT
FROM
	APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_SNAP_1
*/	
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

-- RUN duplicate detector to be sure you haven't created duplicates in History View
SELECT count(*)
     , ID
     , _SDC_EXTRACTED_AT 
FROM APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_HISTORY
group by ID
       , _SDC_EXTRACTED_AT 
having count(*) > 1
 -- 0
;
select count(distinct ID)
FROM APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_HISTORY
 -- 11
;
select count(distinct ID, _SDC_EXTRACTED_AT)
FROM APTTUS_DW.SNAPSHOTS.CURRENCYTYPE_HISTORY
 -- 11 
;


