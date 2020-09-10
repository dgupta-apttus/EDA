DROP VIEW APTTUS_DW.SF_CONGA1_0."FMA_Feature_Daily_Values" ;

CREATE OR REPLACE VIEW APTTUS_DW.SF_CONGA1_0."FMA_Feature_Daily_Values"  
COMMENT = 'list parameter values (from list of features in product_analytics_featureparameters) for each day, license, and package.  This is a verticle listing see activity pivot for a different view'
AS 
select A.SFFMA__LICENSE__C as LICENSE_ID
     , L.SFLMA__PACKAGE__C 
     , L.PACKAGE_NAMEFX__C as PRODUCT 
     , A.SFFMA__FEATUREPARAMETER__C
     , P.SFFMA__FULLNAME__C
     , CASE
          WHEN A.SFFMA__VALUE__C > -1 
            THEN A.SFFMA__VALUE__C
        ELSE 0    
       END as FEATURE_VALUE
     , CASE
          WHEN A.SFFMA__VALUE__C > -1
            THEN 'Known'
        ELSE 'Unknown'
       END as FEATURE_STATUS       
     , A.ACTIVITY_DATE
     , L.CUSTOMER_ORG_ID__C
from                  APTTUS_DW.SNAPSHOTS.FMA_VALUES_HISTORY A
inner join            APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_CURRENT  L
                 on  A.SFFMA__LICENSE__C = L.ID
inner join            APTTUS_DW.SF_CONGA1_1.PRODUCT_ANALYTICS_FEATUREPARAMETER P
                 ON  L.SFLMA__PACKAGE__C = P.SFFMA__PACKAGE__C
                 AND A.SFFMA__FEATUREPARAMETER__C = P.ID
                 AND P.C2_DAILY_REPORTING = true