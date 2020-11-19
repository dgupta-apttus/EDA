--- no don't drop it DROP TABLE APTTUS_DW.PRODUCT.ACTIVITY_MONTHLY_SUMMARY_RSH; 

CREATE TABLE APTTUS_DW.PRODUCT.ACTIVITY_MONTHLY_SUMMARY_RSH 
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

COPY INTO "APTTUS_DW"."PRODUCT"."ACTIVITY_MONTHLY_SUMMARY_RSH" 
FROM @~/activity_monthly_summary.csv.gz
FILE_FORMAT = ( 
TYPE = CSV 
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
DATE_FORMAT = 'YYYY-MM-DD'
)
;


SHOW tasks IN SNAPSHOTS;