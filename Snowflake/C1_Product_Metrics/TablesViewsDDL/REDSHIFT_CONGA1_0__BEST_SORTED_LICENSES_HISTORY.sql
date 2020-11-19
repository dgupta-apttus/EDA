--- no don't drop it DROP TABLE APTTUS_DW.PRODUCT.APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH; 

CREATE TABLE APTTUS_DW.PRODUCT.BEST_SORTED_LICENSES_HISTORY_RSH 
     ( 
          subscriber_org_id                     VARCHAR, 
          product_line                          VARCHAR, 
          record_timestamp                      TIMESTAMP, 
          license_id                            INTEGER, 
          salesforce_license_id                 VARCHAR, 
          status                                VARCHAR, 
          org_status                            VARCHAR, 
          salesforce_account_id                 VARCHAR, 
          salesforce_account_name               VARCHAR, 
          is_sandbox                            BOOLEAN, 
          package_name                          VARCHAR, 
          predicted_package_namespace           VARCHAR,
          sort_order                            INTEGER, 
          license_seat_type                     VARCHAR, 
          seats                                 NUMERIC(8,0), 
          used_licenses                         NUMERIC(8,0), 
          install_date                          DATE, 
          months_installed                      INTEGER,
          install_date_string                   VARCHAR, 
          uninstall_date                        DATE,
          expiration                            DATE, 
          expiration_date_string                VARCHAR, 
          select1                               INTEGER 
     ) 
;     

PUT file://C:\Users\greg.wilson\Downloads\best_sorted_licenses_history.csv @~
;

LIST @~;

SHOW PARAMETERS LIKE '%date%';

COPY INTO "APTTUS_DW"."PRODUCT"."BEST_SORTED_LICENSES_HISTORY_RSH" 
FROM @~/best_sorted_licenses_history.csv.gz
FILE_FORMAT = ( 
TYPE = CSV 
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
DATE_FORMAT = 'YYYY-MM-DD'
)
;


SHOW tasks IN SNAPSHOTS;