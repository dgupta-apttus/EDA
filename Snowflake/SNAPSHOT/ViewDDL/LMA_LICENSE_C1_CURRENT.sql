            

CREATE OR REPLACE VIEW LMA_LICENSE_C1_CURRENT  
COMMENT = 'get the most current record for each license ID from the history object'
AS 
WITH the_unique_current AS (
	SELECT ID
         , MAX(_SDC_EXTRACTED_AT) AS LAST_EXTRACTED_AT 
    FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY
    WHERE _SDC_EXTRACTED_AT <= CURRENT_TIMESTAMP() 
    GROUP BY ID
)
	SELECT A.*
	FROM            APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_HISTORY A
	INNER JOIN      the_unique_current B 
	            ON  A.ID = B.ID 
	            AND A._SDC_EXTRACTED_AT = B.LAST_EXTRACTED_AT;