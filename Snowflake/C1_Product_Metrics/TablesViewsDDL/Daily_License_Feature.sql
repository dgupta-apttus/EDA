CREATE TABLE DAILY_LICENSE_FEATURE_VALUES 
  ( 
     FEATURE_NAME        VARCHAR(16777216) 
     , FEATURE_PARAMETER VARCHAR(16777216) 
     , LICENSE_ID        VARCHAR(16777216) 
     , FEATURE_VALUE     DOUBLE(0, 0) 
     , ERROR_COUNT       NUMBER 
     , POSITIVE_COUNT    NUMBER 
     , ACTIVITY_DATE     DATE 
     , SOURCE            VARCHAR(2) 
  ); 

--create table APTTUS_DW.SF_CONGA1_0.DAILY_LICENSE_FEATURE_VALUES 
--as
/*

INSERT INTO DAILY_LICENSE_FEATURE_VALUES (FEATURE_NAME, FEATURE_PARAMETER, LICENSE_ID, FEATURE_VALUE, ERROR_COUNT, POSITIVE_COUNT, ACTIVITY_DATE, SOURCE)
        select A.NAME AS FEATURE_NAME
             , A.SFFMA__FEATUREPARAMETER__C AS FEATURE_PARAMETER
             , A.SFFMA__LICENSE__C AS LICENSE_ID
             , CASE 
                 WHEN A.SFFMA__VALUE__C > 0
                   THEN A.SFFMA__VALUE__C
                ELSE 0   
               END  AS FEATURE_VALUE
             , CASE
                 WHEN A.SFFMA__VALUE__C < 0
                   THEN 1
                ELSE 0
               END ERROR_COUNT
             , CASE
                 WHEN A.SFFMA__VALUE__C > 0
                   THEN 1
                ELSE 0
               END POSITIVE_COUNT
             , TO_DATE(A.SYSTEMMODSTAMP) -1 as ACTIVITY_DATE
             , 'C1' as SOURCE
        FROM                APTTUS_DW.SF_CONGA1_0.SFFMA__FEATUREPARAMETERINTEGER__C A 
        WHERE SYSTEMMODSTAMP > CURRENT_DATE
;        
*/

--Delete from APTTUS_DW.SF_CONGA1_0.DAILY_LICENSE_FEATURE_VALUES ;

CREATE TABLE DAILY_LICENSE_FEATURE_STATUS 
  ( 
     SOURCE          VARCHAR(2) 
     , LICENSE_ID    VARCHAR(16777216) 
     , ACTIVITY_DATE DATE 
     , ERROR_PERCENT NUMBER(22, 6) 
     , ACTIVITY      BOOLEAN 
  ); 

--create table APTTUS_DW.SF_CONGA1_0.DAILY_LICENSE_FEATURE_STATUS 
--as
/*
INSERT INTO DAILY_LICENSE_FEATURE_STATUS (SOURCE, LICENSE_ID, ACTIVITY_DATE, ERROR_PERCENT, ACTIVITY)
with get_totals as (
        select SOURCE
             , LICENSE_ID
             , ACTIVITY_DATE
             , SUM(ERROR_COUNT) AS ERRORS
             , SUM(POSITIVE_COUNT) AS POSITIVES
             , COUNT(*) AS FEATURES_REPORTING  
        FROM APTTUS_DW.SF_CONGA1_0.DAILY_LICENSE_FEATURE_VALUES
        WHERE ACTIVITY_DATE = (CURRENT_DATE -1)
        group by SOURCE
             , LICENSE_ID
             , ACTIVITY_DATE
)

        select SOURCE
             , LICENSE_ID
             , ACTIVITY_DATE
             , (ERRORS * 100)/FEATURES_REPORTING as ERROR_PERCENT
             , CASE
                 WHEN POSITIVES > 0 
                   THEN 1::BOOLEAN
                ELSE 0::BOOLEAN
               END AS ACTIVITY
        FROM get_totals 
; 
*/
--DELETE from APTTUS_DW.SF_CONGA1_0.DAILY_LICENSE_FEATURE_STATUS;