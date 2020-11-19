WITH cteuser_a1 AS (
        SELECT * 
        FROM (
                WITH RECURSIVE tree (SOURCE, ID, MANAGERID, NAME, DEPTH, PATH, INDENT, PATHNAME, ISACTIVE, TITLE, DEPARTMENT) AS (
                        SELECT 'A1' SOURCE
                              , ID
                              , MANAGERID
                              , NAME
                              , 1 AS DEPTH
                              , '' AS PATH 
                              , '' INDENT
                              , NAME PATHNAME
                              , ISACTIVE
                              , TITLE
                              , DEPARTMENT
                        FROM APTTUS_DW.SF_PRODUCTION."USER"
                        WHERE ID = (
                                WITH RECURSIVE t3 (SOURCE, ID, MANAGERID) AS (
                                        SELECT 'A1' SOURCE
                                             , ID
                                             , MANAGERID 
                                        FROM APTTUS_DW.SF_PRODUCTION."USER" 
                                        WHERE ID = '0053Z00000KQj6QQAT'
                                UNION ALL
                                        SELECT 'A1' SOURCE
                                             , t2.ID
                                             , t2.MANAGERID 
                                        FROM APTTUS_DW.SF_PRODUCTION."USER" t2
                                        JOIN t3 
                                             ON t3.MANAGERID=t2.ID
                                )
                                SELECT ID 
                                FROM t3 
                                WHERE MANAGERID IS NULL
                        )
                UNION ALL
                        SELECT 'A1' SOURCE
                              , t2.ID
                              , t2.MANAGERID
                              , t2.NAME
                              , tree.DEPTH+1
                              , PATH || '/' || CAST(t2.ID AS VARCHAR)
                              , INDENT || '...'
                              , PATHNAME || '--' || t2.NAME
                              , t2.ISACTIVE
                              , t2.TITLE
                              , t2.DEPARTMENT
                        FROM APTTUS_DW.SF_PRODUCTION."USER" AS t2
                        JOIN tree ON tree.ID = t2.MANAGERID
                )
                SELECT INDENT || NAME, * 
                FROM tree
        )
)
, cteuser_c1 AS (
        SELECT * 
        FROM (
                WITH RECURSIVE treec1 (SOURCE, ID, MANAGERID, NAME, DEPTH, PATH, INDENT, PATHNAME, ISACTIVE, TITLE, DEPARTMENT) AS (
                        SELECT 'C1' SOURCE
                              , ID
                              , MANAGERID
                              , NAME
                              , 1 AS DEPTH
                              , '' AS PATH 
                              , '' INDENT
                              , NAME PATHNAME
                              , ISACTIVE
                              , TITLE
                              , DEPARTMENT
                        FROM APTTUS_DW.SF_CONGA1_1."USER"
                        WHERE ID = (
                                WITH RECURSIVE t3 (SOURCE, ID, MANAGERID) AS (
                                        SELECT 'C1' SOURCE
                                             , ID
                                             , MANAGERID 
                                        FROM APTTUS_DW.SF_CONGA1_1."USER" 
                                        WHERE ID = '00550000003NZ1EAAW'
                                UNION ALL
                                        SELECT 'C1' SOURCE
                                              , t2.ID
                                              , t2.MANAGERID 
                                        FROM APTTUS_DW.SF_CONGA1_1."USER" t2
                                        JOIN t3 
                                             ON t3.MANAGERID=t2.ID
                                )
                                SELECT ID 
                                FROM t3 
                                WHERE MANAGERID IS NULL
                        )
                UNION ALL
                        SELECT 'C1' SOURCE
                              , t2.ID
                              , t2.MANAGERID
                              , t2.NAME
                              , treec1.DEPTH+1
                              , PATH || '/' || CAST(t2.ID AS VARCHAR)
                              , INDENT || '...'
                              , PATHNAME || '--' || t2.NAME
                              , t2.ISACTIVE
                              , t2.TITLE
                              , t2.DEPARTMENT
                        FROM APTTUS_DW.SF_CONGA1_1."USER" t2
                        JOIN treec1 
                        ON treec1.ID = t2.MANAGERID
                )
                SELECT INDENT || NAME, * 
                FROM treec1
        )
)
, cteuser AS (
        SELECT * 
        FROM cteuser_a1
UNION
        SELECT * 
        FROM cteuser_c1
)

SELECT * 
FROM cteuser
WHERE ISACTIVE IN('TRUE')
  AND DEPARTMENT LIKE '%Sales%' 
  AND DEPARTMENT NOT LIKE '%Sales Op%'
 -- AND (   TITLE LIKE '%RVP%' 
 --      OR TITLE LIKE '%President%' 
 --      OR Title LIKE '%Growth%'
 --     )
ORDER BY NAME
;



SELECT 'A1' SOURCE
      , ID
      , MANAGERID 
FROM APTTUS_DW.SF_PRODUCTION."USER" 
WHERE ID = '0053Z00000KQj6QQAT'
UNION ALL
SELECT 'A1' SOURCE
     , t2.ID
     , t2.MANAGERID 
FROM APTTUS_DW.SF_PRODUCTION."USER" AS t2
JOIN t3 ON t3.MANAGERID=t2.ID
;

SELECT *
FROM APTTUS_DW.SF_PRODUCTION."USER" 
WHERE ID = '0053Z00000KQj6QQAT'
;

WITH RECURSIVE t3 (SOURCE, ID, MANAGERID) AS
(
SELECT 'A1' SOURCE
      , ID
      , MANAGERID 
FROM APTTUS_DW.SF_PRODUCTION."USER" WHERE ID = '0053Z00000KQj6QQAT'
UNION ALL
SELECT 'A1' SOURCE
     , t2.ID
     , t2.MANAGERID 
FROM APTTUS_DW.SF_PRODUCTION."USER" AS t2
--JOIN t3 ON t3.MANAGERID=t2.ID
)
SELECT * FROM t3 
--WHERE MANAGERID IS NULL
;

select 'C1' as CRM, ID, NAME, Department
FROM APTTUS_DW.SF_CONGA1_1."USER"
where MANAGERID IS NULL
  and ISACTIVE = true
  AND DEPARTMENT LIKE '%Sales%' 
  AND DEPARTMENT NOT LIKE '%Sales Op%'
  AND DEPARTMENT NOT LIKE 'Salesforce%'
;


SELECT 'C1' SOURCE
     , NAME
     , *
FROM APTTUS_DW.SF_CONGA1_1."USER" 
WHERE ID = '00550000003NZ1EAAW'
;