
with filter_comp as ( 
        select *
        FROM APTTUS_DW.PRODUCT.LMA_LIC_PRODUCTLINE_CURRENT 
        where PRODUCT = 'Conga Composer'
)
   , filter_org as (
        select *
        FROM APTTUS_DW.SF_CONGA1_1.SALESFORCE_ORG__C
        where COMPOSER_LICENSE__C is not null
          and ORG_TYPE__C = 'Production'
)
, full_join as (
        select COALESCE(A.LICENSE_ID, B.COMPOSER_LICENSE__C) AS LICENSE_ID 
               , A.STATUS
               , A.ORG_STATUS
               , A.CUSTOMER_ORG
               , B.CONGA_ENFORCE_LMA__C
               , B.CONGA_ENFORCE_USER_MANAGEMENT__C
               , B.SALESFORCE_ORG_ID_15__C 
               , B.ORG_ID__C
               , A.LICENSE_SEAT_TYPE
               , A.SEATS
               , B.LICENSED_SEATS__C
               , B.CONGA_LICENSES__C
               , A.USED_LICENSES
               , B.USED_LICENSES__C
               , B.CREATEDDATE
               , B.SYSTEMMODSTAMP
        from                  filter_comp A
        full outer join       filter_org B
                          ON A.LICENSE_ID = B.COMPOSER_LICENSE__C
)

select * 
from full_join
where STATUS = 'Active'
  and ORG_STATUS = 'ACTIVE'
  and CONGA_ENFORCE_LMA__C = false
  AND CONGA_LICENSES__C > 0
--where CUSTOMER_ORG in ('00Di0000000ZbEdEAK','00D0b000000CbilEAC')
order by CREATEDDATE desc
;

select *
from APTTUS_DW.SF_CONGA1_1.SALESFORCE_ORG__C
where SALESFORCE_ORG_ID_15__C in ('00Di0000000ZbEd','00D0b000000Cbil')
;

SELECT count(*) , CONGA_ENFORCE_LMA__C
FROM APTTUS_DW.SF_CONGA1_1.SALESFORCE_ORG__C
where COMPOSER_LICENSE__C is not null
group by CONGA_ENFORCE_LMA__C

;

select CREATEDDATE, *
FROM APTTUS_DW.SF_CONGA1_1.SALESFORCE_ORG__C
where COMPOSER_LICENSE__C is not null
  and CONGA_ENFORCE_LMA__C = false
order by 1 desc
;   


select distinct ORG_TYPE__C
FROM APTTUS_DW.SF_CONGA1_1.SALESFORCE_ORG__C
;

select NAME, *
FROM APTTUS_DW.SF_CONGA1_1.SALESFORCE_ORG__C
where upper(NAME) like 'AMGE%'