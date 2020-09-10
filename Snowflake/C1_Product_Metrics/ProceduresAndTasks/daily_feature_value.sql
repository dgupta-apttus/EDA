CREATE or replace PROCEDURE daily_feature_value()
  RETURNS VARCHAR
  LANGUAGE javascript
  AS
  $$
  var rs2 = snowflake.execute( { sqlText: 
      `INSERT INTO DAILY_LICENSE_FEATURE_STATUS (SOURCE, LICENSE_ID, ACTIVITY_DATE, ERROR_PERCENT, ACTIVITY)
with get_new_activity as ( 
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
)   
           
, get_totals as (
        select SOURCE
             , LICENSE_ID
             , ACTIVITY_DATE
             , SUM(ERROR_COUNT) AS ERRORS
             , SUM(POSITIVE_COUNT) AS POSITIVES
             , COUNT(*) AS FEATURES_REPORTING  
        FROM get_new_activity
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
        FROM get_totals;`
       } );   
  
  var rs1 = snowflake.execute( { sqlText: 
      `INSERT INTO DAILY_LICENSE_FEATURE_VALUES (FEATURE_NAME, FEATURE_PARAMETER, LICENSE_ID, FEATURE_VALUE, ERROR_COUNT, POSITIVE_COUNT, ACTIVITY_DATE, SOURCE)
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
        WHERE SYSTEMMODSTAMP > CURRENT_DATE and SFFMA__VALUE__C <> 0;`
       } );
       
    
  return 'Done.';
  $$;
  
--call SF_CONGA1_0.DAILY_FEATURE_VALUE();  

show procedures;

CREATE OR REPLACE TASK APTTUS_DW.SF_CONGA1_0.DAILY_FEATURE_VALUE
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 0 19 * * * UTC' 
AS CALL APTTUS_DW.SF_CONGA1_0.DAILY_FEATURE_VALUE();

show tasks;
describe task APTTUS_DW.SF_CONGA1_0.DAILY_FEATURE_VALUE;
alter task APTTUS_DW.SF_CONGA1_0.DAILY_FEATURE_VALUE suspend;
alter task APTTUS_DW.SF_CONGA1_0.DAILY_FEATURE_VALUE resume;