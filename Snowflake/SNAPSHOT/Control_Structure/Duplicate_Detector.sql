-- ASSET
select count(distinct ID)
FROM APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
;

select count(distinct ID, _SDC_EXTRACTED_AT)
FROM APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
;

SELECT count(*)
     , ID
     , _SDC_EXTRACTED_AT 
FROM APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
group by ID
       , _SDC_EXTRACTED_AT 
having count(*) > 1
;

-- FMA VALUES
select count(distinct SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C)
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
;

select count(distinct SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , _SDC_EXTRACTED_AT)
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
;

SELECT count(*)
      , SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , _SDC_EXTRACTED_AT
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
group by SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , _SDC_EXTRACTED_AT
having count(*) > 1      
;
-- activity_Date may not be unique
SELECT count(*)
      , SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , ACTIVITY_DATE
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
group by SFFMA__LICENSE__C
      , SFFMA__FEATUREPARAMETER__C
      , ACTIVITY_DATE
having count(*) > 1      
;

select *
FROM APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY
WHERE
SFFMA__LICENSE__C IN ('a025000000gYtWsAAK') AND
SFFMA__FEATUREPARAMETER__C IN ('a9v1T0000007o67QAA')
;
-- account
select count(distinct ACCOUNTID_18__C)
FROM APTTUS_DW.SNAPSHOTS.ACCOUNT_C1_HISTORY
;

SELECT COUNT(*) 
     , ACCOUNTID_18__C
     , _SDC_EXTRACTED_AT     
FROM APTTUS_DW.SNAPSHOTS.ACCOUNT_C1_HISTORY
group by ACCOUNTID_18__C
     , _SDC_EXTRACTED_AT 
having count(*) > 1     
;

-- LMA LICENSE C1
select count(distinct ID)
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY
;

select count(distinct ID
      , _SDC_EXTRACTED_AT)
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY
;

SELECT count(*)
     , ID
     , _SDC_EXTRACTED_AT 
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY
group by ID
       , _SDC_EXTRACTED_AT 
having count(*) > 1
;

SELECT count(*)
     , ID
     , ACTIVITY_DATE 
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY
group by ID
       , ACTIVITY_DATE 
having count(*) > 1
;
-- LMA LICENSE CLMCPQ
select count(distinct ID)
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_HISTORY
;

select count(distinct ID
      , _SDC_EXTRACTED_AT)
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_HISTORY
;

SELECT count(*)
     , ID
     , _SDC_EXTRACTED_AT 
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_HISTORY
group by ID
       , _SDC_EXTRACTED_AT 
having count(*) > 1
;

SELECT count(*)
     , ID
     , ACTIVITY_DATE 
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_HISTORY
group by ID
       , ACTIVITY_DATE 
having count(*) > 1
;

SELECT count(*)
     , ID 
FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_CLMCPQ_CURRENT
group by ID
having count(*) > 1
;
-- product2 C1
SELECT count(*)
     , ID
     , _SDC_EXTRACTED_AT 
FROM APTTUS_DW.SNAPSHOTS.PRODUCT2_C1_HISTORY
group by ID
       , _SDC_EXTRACTED_AT 
having count(*) > 1
;

-- opportunity c1
select count(distinct ID)
FROM APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C1_HISTORY
;

select count(distinct ID, Activity_Date )
FROM APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C1_HISTORY
;

SELECT count(*)
     , ID
     , _SDC_EXTRACTED_AT 
FROM APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C1_HISTORY
group by ID
       , _SDC_EXTRACTED_AT 
having count(*) > 1
;



