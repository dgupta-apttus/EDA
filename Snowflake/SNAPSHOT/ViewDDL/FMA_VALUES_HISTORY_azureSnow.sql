SELECT CURRENT_ROLE();

Select TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME, MAX(ORDINAL_POSITION)
 FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME LIKE 'FMA_VALUES_SNAP%'
 GROUP by TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
 order by 1
; 

with get_last as(
        Select MAX(ORDINAL_POSITION) as LAST_COL
         FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
         WHERE TABLE_NAME LIKE 'FMA_VALUES_SNAP_1'
)
Select TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME
 FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME LIKE 'FMA_VALUES_SNAP%'
   AND ORDINAL_POSITION > ((SELECT LAST_COL from get_last)-1)
   AND COLUMN_NAME <> 'SNAP_LOAD_AT'
;   
         
Select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION
 FROM APTTUS_DW.INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME LIKE 'FMA_VALUES_SNAP%'
 order by ORDINAL_POSITION, TABLE_NAME
  ;

select count(*) from APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1;
select count(*) from APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1603977631784_1604332875903;
select count(*) from APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1604747110806_1604764881711;	

-- CHANGE -- uncommend insert and update table name to previous snap
-- CHANGE -- manage columns near bottom
INSERT INTO  APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1603977631784_1604332875903 
         (CREATEDBYID, CREATEDDATE, ID, ISDELETED, LASTMODIFIEDBYID, LASTMODIFIEDDATE, NAME
         , SFFMA__FEATUREPARAMETER__C, SFFMA__LICENSE__C, SFFMA__VALUE__C, SYSTEMMODSTAMP
         , _SDC_BATCHED_AT, _SDC_EXTRACTED_AT, _SDC_RECEIVED_AT, _SDC_SEQUENCE, _SDC_TABLE_VERSION
         , SFFMA__FULLNAME__C
         , SNAP_LOAD_AT) 
SELECT CREATEDBYID, CREATEDDATE, ID, ISDELETED, LASTMODIFIEDBYID, LASTMODIFIEDDATE, NAME
         , SFFMA__FEATUREPARAMETER__C, SFFMA__LICENSE__C, SFFMA__VALUE__C, SYSTEMMODSTAMP
         , _SDC_BATCHED_AT, _SDC_EXTRACTED_AT, _SDC_RECEIVED_AT, _SDC_SEQUENCE, _SDC_TABLE_VERSION
         , NULL AS SFFMA__FULLNAME__C
-- change -- remove older NULL AS from column
-- change -- add columns from the previous snap iteration as NULL AS 
         , SNAP_LOAD_AT 
FROM  APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1
;

-- change -- check counts to insure that the recently replaced snap now has all rows
DROP TABLE APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1;
-- change first snap to be the recently replaced 
alter table APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1603977631784_1604332875903 rename to APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1;

select count(*) from APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1;

--DROP VIEW APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY ;

-- make changes below and then rebuild the history
CREATE OR REPLACE VIEW APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY  
COMMENT = 'Union FMA_VALUE snapshots to make complete history'
AS 
WITH the_union AS (
	SELECT
		CREATEDBYID
		, CREATEDDATE
		, ID
		, ISDELETED
		, LASTMODIFIEDBYID
		, LASTMODIFIEDDATE
		, "NAME"
		, SFFMA__FEATUREPARAMETER__C
		, SFFMA__LICENSE__C
		, SFFMA__VALUE__C
		, SYSTEMMODSTAMP
		, "_SDC_BATCHED_AT"
		, "_SDC_EXTRACTED_AT"
		, "_SDC_RECEIVED_AT"
		, "_SDC_SEQUENCE"
		, "_SDC_TABLE_VERSION"
		, SFFMA__FULLNAME__C
-- change -- add latest columns  		
		, SNAP_LOAD_AT
	        , to_date(SYSTEMMODSTAMP) AS ACTIVITY_DATE		
	FROM
-- change from to latest iteration of snap	
		APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1604747110806_1604764881711
UNION
    SELECT
		CREATEDBYID
		, CREATEDDATE
		, ID
		, ISDELETED
		, LASTMODIFIEDBYID
		, LASTMODIFIEDDATE
		, "NAME"
		, SFFMA__FEATUREPARAMETER__C
		, SFFMA__LICENSE__C
		, SFFMA__VALUE__C
		, SYSTEMMODSTAMP
		, "_SDC_BATCHED_AT"
		, "_SDC_EXTRACTED_AT"
		, "_SDC_RECEIVED_AT"
		, "_SDC_SEQUENCE"
		, "_SDC_TABLE_VERSION"
		, SFFMA__FULLNAME__C		
-- change -- remove NULL AS from previous round
-- change -- ADD newest Columns as NULL AS  		
		, SNAP_LOAD_AT
		, to_date(SYSTEMMODSTAMP) AS ACTIVITY_DATE
	FROM
		APTTUS_DW.SNAPSHOTS.FMA_VALUES_SNAP_1
)
, the_unique AS (
	SELECT SFFMA__LICENSE__C
         , SFFMA__FEATUREPARAMETER__C
         , ACTIVITY_DATE
         , MAX(SNAP_LOAD_AT) AS SNAP_LOAD_AT
    FROM the_union  
    GROUP BY SFFMA__LICENSE__C
         , SFFMA__FEATUREPARAMETER__C
         , ACTIVITY_DATE
)
	SELECT A.*
	     , to_date(A."_SDC_EXTRACTED_AT") AS EXTRACT_DATE
	     , to_date(A."SNAP_LOAD_AT") AS REPORTING_DATE
	FROM                 the_union A
	INNER JOIN           the_unique B
	             ON  A.SFFMA__LICENSE__C = B.SFFMA__LICENSE__C
	             AND A.SFFMA__FEATUREPARAMETER__C = B.SFFMA__FEATUREPARAMETER__C
	             AND A.ACTIVITY_DATE = B.ACTIVITY_DATE
	             AND A.SNAP_LOAD_AT = B.SNAP_LOAD_AT
;

-- RUN duplicate detector to be sure you haven't created duplicates in History View
SELECT count(*)
      , SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , _SDC_EXTRACTED_AT
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
group by SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , _SDC_EXTRACTED_AT
having count(*) > 1      
-- 0 -- needs to always be zero
;

select count(distinct SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C)
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
-- 1333938
;

select count(distinct SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , _SDC_EXTRACTED_AT)
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
-- 28968139
;


