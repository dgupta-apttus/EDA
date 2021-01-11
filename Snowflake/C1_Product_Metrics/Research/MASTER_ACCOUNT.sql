SELECT distinct TYPE 
FROM APTTUS_DW.SF_PRODUCTION.ACCOUNT
;

SELECT ACCOUNTID_18__C, ID
FROM APTTUS_DW.SF_CONGA1_1.ACCOUNT
WHERE ACCOUNTID_18__C <> ID

;

WITH FULL_OUTER AS (
        select COALESCE(A.MASTER_ID__C, 'Not Mapped') as MASTER_ID
             , COALESCE(A.ID, 'Not Mapped') as C2_ACCOUNT_ID 
             , A.ID AS A1_ACCOUNT_ID
             , B.ID AS C1_ACCOUNT_ID
             , COALESCE(A.TYPE, B.TYPE) as TYPE     
             , COALESCE(A.NAME, 'Not Mapped') AS C2_ACCOUNT_NAME
             , A.NAME AS A1_ACCOUNT_NAME
             , B.NAME as C1_ACCOUNT_NAME
             , A.A1_SURVIVOR_ACCOUNT_ID__C
             , A.C1_ACCOUNT_ID__C AS MAPPED_C1_ACCOUNT_ID
             , CASE
                 WHEN A.C1_ACCOUNT_ID__C = B.ID
                   THEN 'Paired Accounts'
                 WHEN A.ID is not null
                   THEN 'A1 only'
                 WHEN B.ID is not null
                   THEN 'C1 only'
                ELSE 'Nothing found'
               END AS MATCH_TYPE          
        FROM                 APTTUS_DW.SF_PRODUCTION.ACCOUNT A
        FULL OUTER JOIN      APTTUS_DW.SF_CONGA1_1.ACCOUNT B
                         ON A.C1_ACCOUNT_ID__C = B.ID 
        WHERE (A.TYPE <> 'Site' OR A.TYPE is Null)
)
select *
from FULL_OUTER
where MASTER_ID IN ('ACT-430625','ACT-430741') 
;

select MASTER_ID
from FULL_OUTER
GROUP BY MASTER_ID
HAVING COUNT(*) > 1
;
     
SELECT * from FULL_OUTER
;

select COUNT(*), MATCH_TYPE
from FULL_OUTER    
group by MATCH_TYPE    
;