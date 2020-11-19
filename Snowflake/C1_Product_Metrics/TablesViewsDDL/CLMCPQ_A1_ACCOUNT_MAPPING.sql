--DROP VIEW APTTUS_DW.PRODUCT.CLMCPQ_A1_ACCOUNT_MAPPING;

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT.CLMCPQ_A1_ACCOUNT_MAPPING
COMMENT = 'get rid of duplicates for account mappings between old and new Salesforce instances
this is method 1 -- may need to update order by in partition if we get updates on how to choose the best answer
'
AS 
with select1 as (
        select X18_DIGIT_OLD_SFDC_ID__C 
             , ID 
             , ROW_NUMBER () 
                    OVER (PARTITION BY X18_DIGIT_OLD_SFDC_ID__C ORDER BY CREATEDDATE ASC, ID ASC) AS SELECT1_FOR_CLMCPQ 
        FROM APTTUS_DW.SF_PRODUCTION.ACCOUNT
        where X18_DIGIT_OLD_SFDC_ID__C is not null
)
        select X18_DIGIT_OLD_SFDC_ID__C as CLMCPQ_ACCOUNT_ID
              , ID AS A1_ACCOUNT_ID
        from select1
        where SELECT1_FOR_CLMCPQ = 1
;
