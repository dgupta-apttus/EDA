--DROP VIEW APTTUS_DW.PRODUCT."FMA_Rolling_Activity";  

CREATE OR REPLACE VIEW APTTUS_DW.PRODUCT."FMA_Rolling_Activity"  
COMMENT = 'Pivot the rolling activity values from FMA_Feature_Daily_Values for Orchestrate and Grid'
AS 
WITH reduce AS (
     SELECT CUSTOMER_ORG_ID__C AS SOURCE_ORG_ID
	 , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT_LINE
         , ACTIVITY_DATE
         , SFFMA__FULLNAME__C
         , FEATURE_VALUE
    FROM APTTUS_DW.PRODUCT."FMA_Feature_Daily_Values"   
    WHERE SFLMA__PACKAGE__C IN ('a015000000u3HKfAAM','a0150000017jcZWAAY','a015000000xCYUxAAO')
      AND SFFMA__FULLNAME__C IN ( 'FSTR__Total_Tasks_90'
                                 ,'FSTR__Distinct_Users_30'  
                                 ,'CRMC_PP__Views_Loaded'
                                 ,'CRMC_PP__Monthly_Active_Users'
                                 ,'APXT_Redlining__Monthly_Active_Users'
                                 ,'APXT_Redlining__Send_for_Negotiation_Events'
                                 ,'APXT_Redlining__View_Redlines_Page_Events'
                                 )
      AND ACTIVITY_DATE > '2020-09-01'
      AND FEATURE_STATUS = 'Known'
)   
, pivot_contract_events AS (
	SELECT *
	FROM reduce
	pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('APXT_Redlining__Monthly_Active_Users','APXT_Redlining__Send_for_Negotiation_Events','APXT_Redlining__View_Redlines_Page_Events'))
      as p (SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT_LINE
         , ACTIVITY_DATE
         , ROLLING_ACTIVE_USERS
         , SEND_FOR_NEGOTIATION
         , VIEW_REDLINES_PAGE         
         )
    WHERE SFLMA__PACKAGE__C = 'a015000000xCYUxAAO'  
)
, calculate_lags AS (
    SELECT SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT_LINE      
         , ACTIVITY_DATE   
         , SEND_FOR_NEGOTIATION 
         , coalesce(lag(SEND_FOR_NEGOTIATION,1) over (partition by SOURCE_ORG_ID order by SOURCE_ORG_ID, ACTIVITY_DATE),0) as PREV_SEND_FOR_NEGOTIATION      
         , VIEW_REDLINES_PAGE
         , coalesce(lag(VIEW_REDLINES_PAGE,1) over (partition by SOURCE_ORG_ID order by SOURCE_ORG_ID, ACTIVITY_DATE),0) AS PREV_VIEW_REDLINES_PAGE
    from pivot_contract_events     
)
, contract_events_daily AS (
    SELECT SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT_LINE    
         , ACTIVITY_DATE
         , case 
                 WHEN ACTIVITY_DATE = '2020-09-03'
                   THEN 0
                 WHEN SEND_FOR_NEGOTIATION >= PREV_SEND_FOR_NEGOTIATION
                   THEN (SEND_FOR_NEGOTIATION - PREV_SEND_FOR_NEGOTIATION) 
                 ELSE SEND_FOR_NEGOTIATION   
           END AS SEND_FOR_NEGOTIATION_EVENTS     
         , case
                 WHEN ACTIVITY_DATE = '2020-09-03'
                   THEN 0
                 WHEN VIEW_REDLINES_PAGE >= PREV_VIEW_REDLINES_PAGE
                   THEN (VIEW_REDLINES_PAGE - PREV_VIEW_REDLINES_PAGE) 
                 ELSE VIEW_REDLINES_PAGE  
           END AS VIEW_REDLINES_PAGE_EVENTS         
    FROM calculate_lags 
)
, union3 AS ( 
    SELECT *
	FROM reduce
	pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('CRMC_PP__Views_Loaded','CRMC_PP__Monthly_Active_Users'))
      as p (SOURCE_ORG_ID
	 , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT_LINE
         , ACTIVITY_DATE
         , ROLLING_ACTIVITY_COUNT
         , ROLLING_ACTIVE_USERS
         )
    WHERE SFLMA__PACKAGE__C = 'a015000000u3HKfAAM' 
UNION
	SELECT *
	FROM reduce
	pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('FSTR__Total_Tasks_90','FSTR__Distinct_Users_30'))
      as p (SOURCE_ORG_ID
	 , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT_LINE
         , ACTIVITY_DATE
         , ROLLING_ACTIVITY_COUNT
         , ROLLING_ACTIVE_USERS
         )
    WHERE SFLMA__PACKAGE__C = 'a0150000017jcZWAAY' 
UNION
    SELECT SOURCE_ORG_ID
	 , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT_LINE
         , ACTIVITY_DATE
         , COALESCE(SEND_FOR_NEGOTIATION + VIEW_REDLINES_PAGE, 0)  AS ROLLING_ACTIVITY_COUNT
         , ROLLING_ACTIVE_USERS    
    FROM                     pivot_contract_events
)
	SELECT A.SOURCE_ORG_ID
	     , A.LICENSE_ID
         , A.SFLMA__PACKAGE__C
         , A.PRODUCT_LINE
         , A.ACTIVITY_DATE
         , COALESCE(A.ROLLING_ACTIVITY_COUNT, 0) AS ROLLING_ACTIVITY_COUNT
         , COALESCE(A.ROLLING_ACTIVE_USERS, 0) AS ROLLING_ACTIVE_USERS
         , CASE  
             WHEN A.SFLMA__PACKAGE__C = 'a015000000xCYUxAAO'
               THEN COALESCE(B.SEND_FOR_NEGOTIATION_EVENTS + B.VIEW_REDLINES_PAGE_EVENTS, 0) 
            ELSE NULL
           END AS CONTRACTS4SF_DAILY_ACTIVITY  
    FROM                         union3 A
    LEFT OUTER JOIN             contract_events_daily B 
                     ON  A.SOURCE_ORG_ID = B.SOURCE_ORG_ID
	                 AND A.LICENSE_ID = B.LICENSE_ID
                     AND A.SFLMA__PACKAGE__C = B.SFLMA__PACKAGE__C
                     AND A.PRODUCT_LINE = B.PRODUCT_LINE
                     AND A.ACTIVITY_DATE = B.ACTIVITY_DATE
;	


SELECT * FROM APTTUS_DW.PRODUCT."FMA_Rolling_Activity" 
WHERE PRODUCT = 'Contracts4SF' 
--LICENSE_ID = 'a021T00000w44tXQAQ'
--FEATURE_STATUS = 'Unknown'
--AND 
--ROLLING_ACTIVE_USERS > 0
--PRODUCT = 'Contracts4SF'
--AND (ROLLING_ACTIVE_USERS IS NULL OR ROLLING_ACTIVITY_COUNT IS NULL) 
ORDER BY 2,5;  