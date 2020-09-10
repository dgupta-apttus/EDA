DROP TABLE APTTUS_DW.REDSHIFT_CONGA1_0.ACTIVITY_MONTHLY_SUMMARY; 

CREATE TABLE APTTUS_DW.REDSHIFT_CONGA1_0.ACTIVITY_MONTHLY_SUMMARY 
	(                          

      ORG_SOURCE                    VARCHAR,
      source_org_id                 VARCHAR,  
      salesforce_org_id             VARCHAR,
      activity_year                 SMALLINT, 
      activity_month                SMALLINT, 
      activity_month_date           DATE,
      product_line                  VARCHAR,
      activity_count                bigint, 
      unique_users                  bigint, 
      is_sandbox_edition            BOOLEAN,
      environment_id                INTEGER, 
      activity_month_date_id        INTEGER      

     ) 
;     

PUT file://C:\Users\greg.wilson\Documents\C_Projects\ToSnowFlake\activity_monthly_summary.csv @~;

LIST @~;

SHOW PARAMETERS LIKE '%date%';

COPY INTO "APTTUS_DW"."REDSHIFT_CONGA1_0"."ACTIVITY_MONTHLY_SUMMARY" 
FROM @~/activity_monthly_summary.csv.gz
FILE_FORMAT = ( 
TYPE = CSV 
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
DATE_FORMAT = 'YYYY-MM-DD'
)
;


SHOW tasks IN SNAPSHOTS;