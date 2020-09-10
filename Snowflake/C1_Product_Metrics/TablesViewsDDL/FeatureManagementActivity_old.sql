
DROP view APTTUS_DW.SF_CONGA1_0."FeatureManagementActivity";

CREATE OR REPLACE VIEW APTTUS_DW.SF_CONGA1_0."FeatureManagementActivity"  
COMMENT = 'Provide Dashboard to see that Feature Management Activity is Accumulating' 
AS 

with step1 as (
        SELECT 
               ACTIVITY_DATE
             , SOURCE  
             , LICENSE_ID
             , L.PACKAGE_NAMEFX__C as PRODUCT 
             , ERROR_PERCENT::DECIMAL(5,2) as ERROR_PERCENT
             , CASE
                   WHEN ACTIVITY = true
                     Then 1
                else 0     
               END as WITH_ACTIVITY
             , L.SFLMA__PACKAGE_VERSION_NUMBER__C    
        FROM                   DAILY_LICENSE_FEATURE_STATUS A
        left outer join        APTTUS_DW.SF_CONGA1_0.SFLMA__LICENSE__C  L
                          on A.LICENSE_ID = L.ID
)

SELECT 
       ACTIVITY_DATE
     , SOURCE  
     , PRODUCT 
     , COUNT(*) as LICENSES_WITH_ROWS 
     , AVG(ERROR_PERCENT)::DECIMAL(5,2) as AVG_FMA_PARAMETER_ERROR
     , SUM(WITH_ACTIVITY) as LICENSES_WITH_ACTIVITY 
FROM step1
group by ACTIVITY_DATE, SOURCE, PRODUCT 
;

SHOW GRANTS ON APTTUS_DW.SF_CONGA1_0."FeatureManagementActivity";
GRANT SELECT ON APTTUS_DW.SF_CONGA1_0."FeatureManagementActivity" TO SYSADMIN with GRANT OPTION;
SHOW GRANTS ON APTTUS_DW.SF_CONGA1_0."FeatureManagementActivity";