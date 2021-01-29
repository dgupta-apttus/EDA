SELECT *
FROM APTTUS_DW.SF_PRODUCTION."Account_C2"
where ACCOUNT_NAME like 'Mong%'
;

-- org = 00DA0000000Kz0lMAC
with get_recent as (
SELECT MAX(SNAP_LOAD_AT) as SNAP_LOAD_AT, ID
  FROM APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY
GROUP BY ID  
)

SELECT A.*
FROM                   APTTUS_DW.SNAPSHOTS.ASSET_C1_HISTORY A
--INNER JOIN             get_recent B
--          ON  A.ID = B.ID
--          AND A.SNAP_LOAD_AT = B.SNAP_LOAD_AT 	
WHERE A.ACCOUNTID = '0015000000ceqa0AAA'
  AND A.ENTITLEMENT_STATUS__C = 'Active'
;

SELECT A.*
FROM                   APTTUS_DW.SF_CONGA1_1.ASSET A
--INNER JOIN             get_recent B
--          ON  A.ID = B.ID
--          AND A.SNAP_LOAD_AT = B.SNAP_LOAD_AT 	
WHERE A.ACCOUNTID = '0015000000ceqa0AAA'
  AND A.ENTITLEMENT_STATUS__C = 'Active'
;

select * 
from APTTUS_DW.PRODUCT.LMA_LICENSE_DIM
where "License ID" NOT IN (select distinct "License ID" from APTTUS_DW.PRODUCT."Monthly_Score_Package_Org")
;
select *
from APTTUS_DW.PRODUCT."Monthly_Score_Package_Org"
--WHERE "License ID" is null
WHERE ACTIVITY_COUNT = 0
  and "License ID" is not null
;

select * from  APTTUS_DW.PRODUCT.LMA_LICENSE_DIM
where "License ID" = 'a021T00000qUczjQAC'
;

select * 
from APTTUS_DW.PRODUCT."Monthly_Score_Package_Org"
where "License ID" = 'a021T00000qUczjQAC'
;

select A.*, B."License ID" 
from             APTTUS_DW.PRODUCT.LMA_LICENSE_DIM A
left outer join  APTTUS_DW.PRODUCT."Monthly_Score_Package_Org" B
               ON A."License ID" = B."License ID"
      WHERE  B."License ID" is null         
;
